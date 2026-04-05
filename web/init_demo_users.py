#!/usr/bin/env python3
"""
init_demo_users.py — Create demo users for testing the dashboard

Creates 5 demo users with different permission levels and city/agent access.

Usage:
  python web/init_demo_users.py
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.web_db import get_db_connection
from web.auth import hash_password

# Demo users to create
DEMO_USERS = [
    {
        "username": "admin",
        "email": "admin@insulleads.local",
        "password": "admin123",
        "full_name": "Administrator",
        "roles": ["admin"],
        "city_ids": [],  # Empty = all cities
        "agent_ids": [],  # Empty = all agents
    },
    {
        "username": "manager",
        "email": "manager@insulleads.local",
        "password": "manager123",
        "full_name": "Sales Manager",
        "roles": ["manager"],
        "city_ids": [],  # Empty = all cities
        "agent_ids": [],  # Empty = all agents
    },
    {
        "username": "sf_permits",
        "email": "sf.permits@insulleads.local",
        "password": "sfpermits123",
        "full_name": "SF Permits Specialist",
        "roles": ["user"],
        "city_ids": [44],  # San Francisco only (assuming city id 44)
        "agent_ids": [1],  # Permits agent only (assuming agent id 1)
    },
    {
        "username": "solar_team",
        "email": "solar@insulleads.local",
        "password": "solar123",
        "full_name": "Solar Lead Hunter",
        "roles": ["user"],
        "city_ids": [],  # All cities
        "agent_ids": [2],  # Solar agent only (assuming agent id 2)
    },
    {
        "username": "viewer",
        "email": "viewer@insulleads.local",
        "password": "viewer123",
        "full_name": "Read-Only Viewer",
        "roles": ["viewer"],
        "city_ids": [44, 45, 46],  # San Francisco, San Jose, Oakland (sample)
        "agent_ids": [1, 2, 3],  # Permits, Solar, Rodents (sample)
    },
]


def create_demo_users():
    """Create all demo users in database."""
    conn = get_db_connection()
    c = conn.cursor()

    created_count = 0
    skipped_count = 0

    for user_data in DEMO_USERS:
        username = user_data["username"]

        # Check if user already exists
        c.execute("SELECT id FROM users WHERE username = ?", (username,))
        existing = c.fetchone()

        if existing:
            print(f"⏭️  User '{username}' already exists (skipping)")
            skipped_count += 1
            continue

        # Create user
        password_hash = hash_password(user_data["password"])
        c.execute(
            """
            INSERT INTO users (username, email, password_hash, full_name)
            VALUES (?, ?, ?, ?)
            """,
            (username, user_data["email"], password_hash, user_data["full_name"]),
        )

        user_id = c.lastrowid

        # Assign roles
        for role_name in user_data["roles"]:
            c.execute("SELECT id FROM roles WHERE name = ?", (role_name,))
            role_id = c.fetchone()[0]
            c.execute(
                "INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)",
                (user_id, role_id),
            )

        # Assign city access
        for city_id in user_data["city_ids"]:
            c.execute(
                "INSERT INTO user_city_access (user_id, city_id) VALUES (?, ?)",
                (user_id, city_id),
            )

        # Assign agent access
        for agent_id in user_data["agent_ids"]:
            c.execute(
                "INSERT INTO user_agent_access (user_id, agent_id) VALUES (?, ?)",
                (user_id, agent_id),
            )

        print(f"✓ Created user '{username}' (roles: {', '.join(user_data['roles'])})")
        created_count += 1

    conn.commit()
    conn.close()

    print()
    print("=" * 60)
    print(f"✅ Demo user creation complete: {created_count} created, {skipped_count} skipped")
    print("=" * 60)
    print()
    print("📝 Demo User Credentials:")
    print()

    for user in DEMO_USERS:
        roles = ", ".join(user["roles"])
        cities = f"{len(user['city_ids'])} cities" if user['city_ids'] else "All cities"
        agents = f"{len(user['agent_ids'])} agents" if user['agent_ids'] else "All agents"

        print(f"  Username: {user['username']:<20} Password: {user['password']:<15}")
        print(f"    Role: {roles:<25} City access: {cities:<20} Agent access: {agents}")
        print()


if __name__ == "__main__":
    create_demo_users()
