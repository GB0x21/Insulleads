#!/usr/bin/env python3
"""
utils/fix_sources.py — Re-map Krayin lead sources from SQLite agent_sources data.

Reads each synced lead's agent_sources from SQLite and updates the
corresponding Krayin lead's lead_source_id to match the correct source.

Usage:
    sudo -u insulleads /home/insulleads/Insulleads/venv/bin/python utils/fix_sources.py
"""

import os
import sys
import json
import sqlite3
import subprocess
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.getenv("DB_PATH", "data/leads.db")
CRM_DIR = os.getenv("CRM_DIR", "/home/insulleads/krayin-crm")

SOURCE_NAMES = {
    "permits":        "Permisos de Construccion",
    "solar":          "Solar",
    "rodents":        "Plagas/Roedores",
    "flood":          "Inundacion",
    "construction":   "Construccion Activa",
    "realestate":     "Inmobiliaria",
    "energy":         "Energia",
    "places":         "Google Places",
    "yelp":           "Yelp",
    "deconstruction": "Demolicion",
}


def _read_krayin_env():
    env_path = os.path.join(CRM_DIR, ".env")
    creds = {"host": "127.0.0.1", "port": "3306", "database": "krayin_crm",
             "user": "krayin", "password": ""}
    if not os.path.exists(env_path):
        return creds
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip().strip("'\"")
            if key == "DB_HOST": creds["host"] = val
            elif key == "DB_PORT": creds["port"] = val
            elif key == "DB_DATABASE": creds["database"] = val
            elif key == "DB_USERNAME": creds["user"] = val
            elif key == "DB_PASSWORD": creds["password"] = val
    return creds


def _escape(val):
    if val is None:
        return ""
    return val.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')


def mysql_exec(creds, query):
    cmd = ["mysql", f"-u{creds['user']}", f"-p{creds['password']}",
           f"-h{creds['host']}", f"-P{creds['port']}", creds["database"],
           "-e", query, "-sN"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        print(f"  MySQL error: {result.stderr[:200]}")
        return []
    rows = []
    for line in result.stdout.strip().split("\n"):
        if line:
            rows.append(line.split("\t"))
    return rows


def main():
    creds = _read_krayin_env()
    print("=== Fix Lead Sources ===\n")

    # 1. Ensure all sources exist
    source_ids = {}
    for agent_key, source_name in SOURCE_NAMES.items():
        rows = mysql_exec(creds, f"SELECT id FROM lead_sources WHERE name = '{_escape(source_name)}' LIMIT 1")
        if rows and rows[0]:
            source_ids[agent_key] = int(rows[0][0])
        else:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            mysql_exec(creds, f"INSERT INTO lead_sources (name, created_at, updated_at) VALUES ('{_escape(source_name)}', '{now}', '{now}'); SELECT LAST_INSERT_ID();")
            rows = mysql_exec(creds, f"SELECT id FROM lead_sources WHERE name = '{_escape(source_name)}' LIMIT 1")
            if rows and rows[0]:
                source_ids[agent_key] = int(rows[0][0])
                print(f"  Creado: {source_name} (ID={source_ids[agent_key]})")

    print(f"\nSources disponibles: {len(source_ids)}")
    for k, v in source_ids.items():
        print(f"  {k} → {SOURCE_NAMES[k]} (ID={v})")

    # 2. Read SQLite leads with their agent_sources
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT address_key, agent_sources, krayin_lead_id
        FROM consolidated_leads
        WHERE crm_synced = 1 AND krayin_lead_id IS NOT NULL
    """).fetchall()
    print(f"\nLeads sincronizados en SQLite: {len(rows)}")

    # 3. Create person per source type (for leads without contractor)
    print("\nCreando personas por tipo de fuente...")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    person_ids = {}  # source_name → person_id
    for agent_key, source_name in SOURCE_NAMES.items():
        rows_p = mysql_exec(creds, f"SELECT id FROM persons WHERE name = '{_escape(source_name)}' LIMIT 1")
        if rows_p and rows_p[0]:
            person_ids[agent_key] = int(rows_p[0][0])
        else:
            mysql_exec(creds,
                f"INSERT INTO persons (name, emails, contact_numbers, created_at, updated_at) "
                f"VALUES ('{_escape(source_name)}', '[]', '[]', '{now}', '{now}'); SELECT LAST_INSERT_ID();")
            rows_p = mysql_exec(creds, f"SELECT id FROM persons WHERE name = '{_escape(source_name)}' LIMIT 1")
            if rows_p and rows_p[0]:
                person_ids[agent_key] = int(rows_p[0][0])
                print(f"  Persona creada: {source_name} (ID={person_ids[agent_key]})")

    # 4. Update each lead's source AND person in Krayin
    print("\nActualizando leads...")
    updated = 0
    for row in rows:
        agent_sources = row["agent_sources"] or ""
        krayin_id = row["krayin_lead_id"]
        primary_agent = agent_sources.split(",")[0].strip().lower()
        source_id = source_ids.get(primary_agent)
        person_id = person_ids.get(primary_agent)

        if krayin_id:
            # Check if lead already has a real person (not default)
            lead_rows = mysql_exec(creds,
                f"SELECT p.name FROM leads l JOIN persons p ON l.person_id = p.id "
                f"WHERE l.id = {krayin_id}")
            current_person = lead_rows[0][0] if lead_rows and lead_rows[0] else ""

            updates = []
            if source_id:
                updates.append(f"lead_source_id = {source_id}")
            # Only update person if current is "Propietario Desconocido"
            if person_id and current_person in ("Propietario Desconocido", ""):
                updates.append(f"person_id = {person_id}")

            if updates:
                mysql_exec(creds, f"UPDATE leads SET {', '.join(updates)} WHERE id = {krayin_id}")
                updated += 1

    print(f"Leads actualizados: {updated}")

    # 5. Show distribution
    print("\nDistribucion por fuente:")
    dist = mysql_exec(creds, """
        SELECT s.name, COUNT(l.id)
        FROM leads l
        JOIN lead_sources s ON l.lead_source_id = s.id
        GROUP BY s.name
        ORDER BY COUNT(l.id) DESC
    """)
    for row in dist:
        if len(row) >= 2:
            print(f"  {row[0]}: {row[1]} leads")

    print("\nDistribucion por persona:")
    dist2 = mysql_exec(creds, """
        SELECT p.name, COUNT(l.id)
        FROM leads l
        JOIN persons p ON l.person_id = p.id
        GROUP BY p.name
        ORDER BY COUNT(l.id) DESC
        LIMIT 15
    """)
    for row in dist2:
        if len(row) >= 2:
            print(f"  {row[0]}: {row[1]} leads")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
