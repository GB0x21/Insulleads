"""
web_db.py — Database schema for multi-user dashboard

Extends the existing leads.db with tables for:
- User management (users, roles, permissions)
- City/agent access control
- Session management
- Audit logging
"""

import sqlite3
import os
from datetime import datetime
from pathlib import Path

DB_PATH = os.getenv("DB_PATH", "data/leads.db")


def init_web_db():
    """Initialize web dashboard schema (runs once on app startup)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # ─────────────────────────────────────────────────────
    # Users & Authentication
    # ─────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            full_name TEXT,
            is_active BOOLEAN DEFAULT 1,
            expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Migration: add expires_at column if table already exists without it
    try:
        c.execute("SELECT expires_at FROM users LIMIT 1")
    except sqlite3.OperationalError:
        c.execute("ALTER TABLE users ADD COLUMN expires_at TIMESTAMP")

    # ─────────────────────────────────────────────────────
    # Roles & Permissions
    # ─────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS permissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource TEXT NOT NULL,
            action TEXT NOT NULL,
            description TEXT,
            UNIQUE(resource, action)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS role_permissions (
            role_id INTEGER NOT NULL,
            permission_id INTEGER NOT NULL,
            FOREIGN KEY(role_id) REFERENCES roles(id),
            FOREIGN KEY(permission_id) REFERENCES permissions(id),
            UNIQUE(role_id, permission_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS user_roles (
            user_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(role_id) REFERENCES roles(id),
            UNIQUE(user_id, role_id)
        )
    """)

    # ─────────────────────────────────────────────────────
    # City & Agent Access Control
    # ─────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            state TEXT DEFAULT 'CA',
            county TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS agents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS user_city_access (
            user_id INTEGER NOT NULL,
            city_id INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(city_id) REFERENCES cities(id),
            UNIQUE(user_id, city_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS user_agent_access (
            user_id INTEGER NOT NULL,
            agent_id INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(agent_id) REFERENCES agents(id),
            UNIQUE(user_id, agent_id)
        )
    """)

    # ─────────────────────────────────────────────────────
    # Sessions & Auth Tokens
    # ─────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            access_token TEXT NOT NULL UNIQUE,
            refresh_token TEXT UNIQUE,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    # ─────────────────────────────────────────────────────
    # Audit Logging
    # ─────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT NOT NULL,
            resource_type TEXT,
            resource_id TEXT,
            details TEXT,
            ip_address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    # ─────────────────────────────────────────────────────
    # Lead Contact Log (user interactions)
    # ─────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS lead_contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            lead_id TEXT NOT NULL,
            contact_type TEXT,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    # ─────────────────────────────────────────────────────
    # Create indexes for performance
    # ─────────────────────────────────────────────────────
    c.execute("CREATE INDEX IF NOT EXISTS idx_user_roles ON user_roles(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_user_city ON user_city_access(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_user_agent ON user_agent_access(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(access_token)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_logs(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_lead_contacts_user ON lead_contacts(user_id)")

    # ─────────────────────────────────────────────────────
    # Insert default roles
    # ─────────────────────────────────────────────────────
    default_roles = [
        ("admin", "Full access to all features and user management"),
        ("manager", "Can view all leads, manage team members"),
        ("user", "Can view leads filtered by city/agent, contact leads"),
        ("viewer", "Read-only access to leads"),
    ]

    for role_name, description in default_roles:
        c.execute(
            "INSERT OR IGNORE INTO roles (name, description) VALUES (?, ?)",
            (role_name, description),
        )

    # ─────────────────────────────────────────────────────
    # Insert default permissions
    # ─────────────────────────────────────────────────────
    default_permissions = [
        # Lead permissions
        ("leads", "view", "View leads"),
        ("leads", "filter", "Filter leads by city/agent"),
        ("leads", "contact", "Log contact with lead"),
        # User management
        ("users", "create", "Create new users"),
        ("users", "edit", "Edit users"),
        ("users", "delete", "Delete users"),
        ("users", "manage_roles", "Assign roles to users"),
        ("users", "manage_access", "Restrict city/agent access"),
        # Role management
        ("roles", "view", "View roles"),
        ("roles", "create", "Create roles"),
        ("roles", "edit", "Edit roles"),
        # Audit
        ("audit", "view", "View audit logs"),
    ]

    for resource, action, description in default_permissions:
        c.execute(
            "INSERT OR IGNORE INTO permissions (resource, action, description) VALUES (?, ?, ?)",
            (resource, action, description),
        )

    # ─────────────────────────────────────────────────────
    # Assign permissions to roles
    # ─────────────────────────────────────────────────────
    role_perm_map = {
        "admin": [
            ("leads", "view"),
            ("leads", "filter"),
            ("leads", "contact"),
            ("users", "create"),
            ("users", "edit"),
            ("users", "delete"),
            ("users", "manage_roles"),
            ("users", "manage_access"),
            ("roles", "view"),
            ("roles", "create"),
            ("roles", "edit"),
            ("audit", "view"),
        ],
        "manager": [
            ("leads", "view"),
            ("leads", "filter"),
            ("leads", "contact"),
            ("users", "view"),
            ("audit", "view"),
        ],
        "user": [
            ("leads", "view"),
            ("leads", "filter"),
            ("leads", "contact"),
        ],
        "viewer": [
            ("leads", "view"),
            ("leads", "filter"),
        ],
    }

    for role_name, perms in role_perm_map.items():
        c.execute("SELECT id FROM roles WHERE name = ?", (role_name,))
        role_id = c.fetchone()
        if role_id:
            role_id = role_id[0]
            for resource, action in perms:
                c.execute(
                    "SELECT id FROM permissions WHERE resource = ? AND action = ?",
                    (resource, action),
                )
                perm_id = c.fetchone()
                if perm_id:
                    perm_id = perm_id[0]
                    c.execute(
                        "INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?, ?)",
                        (role_id, perm_id),
                    )

    conn.commit()
    conn.close()


def seed_cities_and_agents():
    """Populate cities and agents from current configuration."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # All 54 Bay Area cities (from previous configuration)
    cities = [
        ("Alameda", "CA", "Alameda"),
        ("Albany", "CA", "Alameda"),
        ("Antioch", "CA", "Contra Costa"),
        ("Benicia", "CA", "Solano"),
        ("Berkeley", "CA", "Alameda"),
        ("Brentwood", "CA", "Contra Costa"),
        ("Campbell", "CA", "Santa Clara"),
        ("Clayton", "CA", "Contra Costa"),
        ("Concord", "CA", "Contra Costa"),
        ("Daly City", "CA", "San Mateo"),
        ("Danville", "CA", "Contra Costa"),
        ("Dublin", "CA", "Alameda"),
        ("East Palo Alto", "CA", "San Mateo"),
        ("Fairfield", "CA", "Solano"),
        ("Fremont", "CA", "Alameda"),
        ("Gilroy", "CA", "Santa Clara"),
        ("Hayward", "CA", "Alameda"),
        ("Hercules", "CA", "Contra Costa"),
        ("Hillsborough", "CA", "San Mateo"),
        ("Livermore", "CA", "Alameda"),
        ("Los Altos", "CA", "Santa Clara"),
        ("Los Gatos", "CA", "Santa Clara"),
        ("Martinez", "CA", "Contra Costa"),
        ("Menlo Park", "CA", "San Mateo"),
        ("Milpitas", "CA", "Santa Clara"),
        ("Moraga", "CA", "Contra Costa"),
        ("Morgan Hill", "CA", "Santa Clara"),
        ("Mountain View", "CA", "Santa Clara"),
        ("Napa", "CA", "Napa"),
        ("Newark", "CA", "Alameda"),
        ("Novato", "CA", "Marin"),
        ("Oakland", "CA", "Alameda"),
        ("Oakley", "CA", "Contra Costa"),
        ("Orinda", "CA", "Contra Costa"),
        ("Pacifica", "CA", "San Mateo"),
        ("Palo Alto", "CA", "Santa Clara"),
        ("Petaluma", "CA", "Sonoma"),
        ("Piedmont", "CA", "Alameda"),
        ("Pinole", "CA", "Contra Costa"),
        ("Pittsburg", "CA", "Contra Costa"),
        ("Pleasanton", "CA", "Alameda"),
        ("Redwood City", "CA", "San Mateo"),
        ("Richmond", "CA", "Contra Costa"),
        ("San Francisco", "CA", "San Francisco"),
        ("San Jose", "CA", "Santa Clara"),
        ("San Leandro", "CA", "Alameda"),
        ("San Mateo", "CA", "San Mateo"),
        ("San Rafael", "CA", "Marin"),
        ("Santa Clara", "CA", "Santa Clara"),
        ("Santa Cruz", "CA", "Santa Cruz"),
        ("Saratoga", "CA", "Santa Clara"),
        ("Sonoma", "CA", "Sonoma"),
        ("Sunnyvale", "CA", "Santa Clara"),
        ("Vacaville", "CA", "Solano"),
        ("Vallejo", "CA", "Solano"),
        ("Walnut Creek", "CA", "Contra Costa"),
    ]

    for city_name, state, county in cities:
        c.execute(
            "INSERT OR IGNORE INTO cities (name, state, county) VALUES (?, ?, ?)",
            (city_name, state, county),
        )

    # All 10 agents
    agents = [
        ("permits", "Building and demolition permits"),
        ("solar", "Solar installation leads"),
        ("rodents", "Pest control and rodent complaints"),
        ("flood", "Flood and water damage reports"),
        ("construction", "Active construction projects"),
        ("realestate", "Real estate sales and transfers"),
        ("energy", "Energy efficiency programs"),
        ("places", "Business licenses and permits"),
        ("yelp", "Business directory and reviews"),
        ("deconstruction", "Deconstruction and demolition projects"),
    ]

    for agent_name, description in agents:
        c.execute(
            "INSERT OR IGNORE INTO agents (name, description) VALUES (?, ?)",
            (agent_name, description),
        )

    conn.commit()
    conn.close()


def get_db_connection():
    """Get a database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
