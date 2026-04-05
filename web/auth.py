"""
auth.py — JWT authentication and user management

Handles password hashing, token generation/validation, and user sessions.
"""

import os
import jwt
import bcrypt
from datetime import datetime, timedelta
from functools import wraps
from flask import request, jsonify, g
from utils.web_db import get_db_connection

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-me-in-production")
ACCESS_TOKEN_EXPIRY = int(os.getenv("JWT_ACCESS_EXPIRY", 3600))  # 1 hour
REFRESH_TOKEN_EXPIRY = int(os.getenv("JWT_REFRESH_EXPIRY", 604800))  # 7 days


class AuthError(Exception):
    """Authentication error."""
    pass


def hash_password(password: str) -> str:
    """Hash password using bcrypt."""
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password.encode(), salt).decode()


def verify_password(password: str, password_hash: str) -> bool:
    """Verify password against hash."""
    return bcrypt.checkpw(password.encode(), password_hash.encode())


def generate_tokens(user_id: int) -> tuple:
    """Generate access and refresh tokens for user."""
    now = datetime.utcnow()

    access_payload = {
        "user_id": user_id,
        "type": "access",
        "iat": now,
        "exp": now + timedelta(seconds=ACCESS_TOKEN_EXPIRY),
    }

    refresh_payload = {
        "user_id": user_id,
        "type": "refresh",
        "iat": now,
        "exp": now + timedelta(seconds=REFRESH_TOKEN_EXPIRY),
    }

    access_token = jwt.encode(access_payload, SECRET_KEY, algorithm="HS256")
    refresh_token = jwt.encode(refresh_payload, SECRET_KEY, algorithm="HS256")

    # Store tokens in database
    conn = get_db_connection()
    c = conn.cursor()
    expires_at = datetime.utcnow() + timedelta(seconds=ACCESS_TOKEN_EXPIRY)

    c.execute(
        """
        INSERT INTO sessions (user_id, access_token, refresh_token, expires_at)
        VALUES (?, ?, ?, ?)
        """,
        (user_id, access_token, refresh_token, expires_at),
    )
    conn.commit()
    conn.close()

    return access_token, refresh_token


def verify_token(token: str) -> dict:
    """Verify and decode JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise AuthError("Token expired")
    except jwt.InvalidTokenError:
        raise AuthError("Invalid token")


def revoke_token(token: str):
    """Revoke a token by deleting it from sessions."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("DELETE FROM sessions WHERE access_token = ? OR refresh_token = ?", (token, token))
    conn.commit()
    conn.close()


def require_auth(f):
    """Decorator to require valid JWT token."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")

        if not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing or invalid authorization header"}), 401

        token = auth_header[7:]  # Remove "Bearer " prefix

        try:
            payload = verify_token(token)

            # Load user from database
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT * FROM users WHERE id = ? AND is_active = 1", (payload["user_id"],))
            user = c.fetchone()
            conn.close()

            if not user:
                return jsonify({"error": "User not found or inactive"}), 401

            # Check if user access has expired
            user_dict = dict(user)
            if user_dict.get("expires_at"):
                expires_at = datetime.strptime(user_dict["expires_at"], "%Y-%m-%d %H:%M:%S")
                if datetime.utcnow() > expires_at:
                    return jsonify({
                        "error": "Access expired",
                        "expired_at": user_dict["expires_at"],
                        "message": "Your access has expired. Contact your administrator."
                    }), 403

            # Store user in Flask g object for request context
            g.user = user_dict
            g.user_id = user["id"]

        except AuthError as e:
            return jsonify({"error": str(e)}), 401

        return f(*args, **kwargs)

    return decorated


def get_user_permissions(user_id: int) -> set:
    """Get all permissions for a user (from roles + custom)."""
    conn = get_db_connection()
    c = conn.cursor()

    # Get permissions from roles
    c.execute("""
        SELECT DISTINCT p.resource || ':' || p.action as perm
        FROM users u
        JOIN user_roles ur ON u.id = ur.user_id
        JOIN role_permissions rp ON ur.role_id = rp.role_id
        JOIN permissions p ON rp.permission_id = p.id
        WHERE u.id = ?
    """, (user_id,))

    perms = {row[0] for row in c.fetchall()}
    conn.close()

    return perms


def get_user_cities(user_id: int) -> list:
    """Get all cities accessible by user."""
    conn = get_db_connection()
    c = conn.cursor()

    # Check if user has explicit city restrictions
    c.execute("""
        SELECT city_id FROM user_city_access WHERE user_id = ?
    """, (user_id,))

    restricted = c.fetchall()

    if restricted:
        # User has explicit restrictions - return only those cities
        city_ids = [row[0] for row in restricted]
        c.execute(f"""
            SELECT id, name FROM cities WHERE id IN ({','.join('?' * len(city_ids))})
        """, city_ids)
    else:
        # No restrictions - can see all cities
        c.execute("SELECT id, name FROM cities")

    cities = [dict(row) for row in c.fetchall()]
    conn.close()

    return cities


def get_user_agents(user_id: int) -> list:
    """Get all agents accessible by user."""
    conn = get_db_connection()
    c = conn.cursor()

    # Check if user has explicit agent restrictions
    c.execute("""
        SELECT agent_id FROM user_agent_access WHERE user_id = ?
    """, (user_id,))

    restricted = c.fetchall()

    if restricted:
        # User has explicit restrictions - return only those agents
        agent_ids = [row[0] for row in restricted]
        c.execute(f"""
            SELECT id, name FROM agents WHERE id IN ({','.join('?' * len(agent_ids))})
        """, agent_ids)
    else:
        # No restrictions - can see all agents
        c.execute("SELECT id, name FROM agents")

    agents = [dict(row) for row in c.fetchall()]
    conn.close()

    return agents


def check_permission(user_id: int, resource: str, action: str) -> bool:
    """Check if user has specific permission."""
    perms = get_user_permissions(user_id)
    return f"{resource}:{action}" in perms
