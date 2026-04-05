# 🔗 Integration: Web Dashboard + Lead Generation System

How the multi-user dashboard integrates with the existing Insulleads lead generation platform.

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│         Insulleads Lead Generation Platform         │
├─────────────────────────────────────────────────────┤
│  main.py (10 agents) → consolidated_leads table    │
│  - Permits Agent                                    │
│  - Solar Agent                                      │
│  - Rodents Agent                                    │
│  - Flood Agent                                      │
│  - Construction Agent                              │
│  - Real Estate Agent                                │
│  - Energy Agent                                     │
│  - Places Agent                                     │
│  - Yelp Agent                                       │
│  - Deconstruction Agent                             │
│                                                     │
│  Outputs: consolidated_leads + signal tracking     │
│  Notifications: Telegram, Email, WhatsApp, Slack  │
└─────────────────────────────────────────────────────┘
                          ↓
          Shared SQLite Database (data/leads.db)
                          ↓
┌─────────────────────────────────────────────────────┐
│    Multi-User Dashboard (New)                       │
├─────────────────────────────────────────────────────┤
│  Flask Web Server (port 5000)                       │
│                                                     │
│  ✓ User Management (users, roles, permissions)     │
│  ✓ Access Control (city-level, agent-level)        │
│  ✓ JWT Authentication                              │
│  ✓ Lead Filtering & Search                         │
│  ✓ Contact Tracking (lead_contacts table)          │
│  ✓ Audit Logging                                   │
│  ✓ Dashboard Stats                                 │
│  ✓ Responsive Web UI                               │
└─────────────────────────────────────────────────────┘
```

## Shared Database

Both systems use the same `data/leads.db` SQLite database:

### Lead Generation Agents Write to:
- `consolidated_leads` — Merged leads from all agents
- `property_signals` — Lead scoring and hot zone signals

### Web Dashboard Reads/Writes to:
- `consolidated_leads` — Display leads to users
- `lead_contacts` — Track when users interact with leads
- `audit_logs` — Log user actions
- **New tables:** users, roles, permissions, cities, agents, sessions

### No Conflicts
✓ Both systems can run simultaneously  
✓ Lead agents write, dashboard reads (no write conflicts)  
✓ Dashboard contact tracking is separate from agent system  
✓ Single source of truth for leads

## Data Flow

### Lead Generation Flow (Existing)

```
1. Agent fetches leads from APIs
2. Deduplication engine filters duplicates
3. Hot zone detector identifies clusters
4. Lead is stored in consolidated_leads
5. Telegram notification sent immediately
6. User sees notification in Telegram chat
```

### Dashboard Flow (New)

```
1. User logs in at http://localhost:5000
2. Dashboard queries consolidated_leads table
3. Filters by user's city/agent access
4. Displays leads in table with stats
5. User can view details, log contact
6. Contact logged in lead_contacts table
7. Audit event logged in audit_logs
8. Stats updated in real-time
```

### Combined Flow

```
Lead Generation (Telegram)         Web Dashboard (Browser)
        ↓                                 ↓
    Agent finds lead          User logs into dashboard
        ↓                                 ↓
  Stores in DB               Queries consolidated_leads
        ↓                                 ↓
 Sends Telegram                  Filters by access level
        ↓                                 ↓
                    Both see same lead data
                              ↓
                        User contacts lead
                              ↓
                      logged in lead_contacts
                              ↓
                    Audit event recorded
```

## Key Integration Points

### 1. consolidated_leads Table

**Agent system writes:**
```python
# From dedup.py
lead = {
    "address": "123 Main St",
    "city": 44,  # City ID
    "score": 85,
    "value": 150000,
    "source": "permits_agent",
    "description": "Building permit filed",
    "source_url": "https://..."
}
# Stored with created_at timestamp
```

**Dashboard reads:**
```sql
SELECT * FROM consolidated_leads
WHERE city IN (user_accessible_cities)
AND source LIKE ? FOR user_accessible_agents
ORDER BY created_at DESC
```

### 2. Lead Scoring (0-100)

Both systems use the same scoring logic:

```python
# From utils/lead_scoring.py
score = base_score + \
        geography_bonus + \
        agent_bonus + \
        recency_bonus

# Dashboard displays with color coding:
# 80-100: HOT (red)
# 60-79:  WARM (orange)
# 40-59:  MEDIUM (yellow)
# 20-39:  COOL (cyan)
# 0-19:   COLD (gray)
```

### 3. City and Agent Reference Data

**Auto-populated on startup:**

```python
# In web_db.py - seed_cities_and_agents()

# All 54 Bay Area cities
CITIES = [
    ("Alameda", "CA", "Alameda County"),
    ("Oakland", "CA", "Alameda County"),
    # ... 52 more
]

# All 10 agent types
AGENTS = [
    ("permits", "Building and demolition permits"),
    ("solar", "Solar installation leads"),
    # ... 8 more
]
```

When a new agent is added to `main.py`, it's automatically available in dashboard filters on next restart.

### 4. User Access Control

**Scenario: Restrict user to specific cities/agents**

```bash
# Admin creates user: "sf_permits"
POST /api/admin/users
{
  "username": "sf_permits",
  "city_ids": [44],        # Only San Francisco
  "agent_ids": [1, 10]     # Only permits + deconstruction
}

# Dashboard will:
# ✓ Only show leads where city=44
# ✓ Only show leads where source IN (permits, deconstruction)
# ✓ Filter dropdowns to show only these options
```

**Query enforcement:**
```python
# In app.py - list_leads()

# Build WHERE clause based on user's access
city_ids = get_user_cities(user_id)  # Returns [44]
agent_names = get_user_agents(user_id)  # Returns [permits, deconstruction]

# Force filter even if user tries to request other cities
WHERE city IN (44) AND (source LIKE 'permits%' OR source LIKE 'deconstruction%')
```

## Running Both Systems

### Terminal 1: Lead Generation

```bash
# Activate venv
source venv/bin/activate

# Start agents (background)
python main.py &

# Tail logs
tail -f logs/agents.log
```

### Terminal 2: Web Dashboard

```bash
# Activate same venv
source venv/bin/activate

# Start web server
python web_server.py

# Server runs on http://localhost:5000
# Login: admin / admin123
```

Both systems can run simultaneously without conflicts.

### Production: systemd Services

**Agent service** (`/etc/systemd/system/insulleads.service`):
```ini
ExecStart=/home/insulleads/Insulleads/venv/bin/python /home/insulleads/Insulleads/main.py
```

**Web service** (`/etc/systemd/system/insulleads-web.service`):
```ini
ExecStart=/home/insulleads/Insulleads/venv/bin/gunicorn -w 4 -b 0.0.0.0:5000 web_server:app
```

**Start both:**
```bash
sudo systemctl start insulleads insulleads-web
sudo systemctl status insulleads insulleads-web
```

## Database Schema Integration

### Lead Data (Shared)

```
consolidated_leads
├── id (primary key)
├── address (shared)
├── city (shared - ID reference to cities table)
├── score (shared - 0-100)
├── value (shared - estimated value)
├── source (shared - agent name)
├── description (shared)
├── source_url (shared)
├── created_at (shared)
└── data (JSON - merged from multiple sources)
```

### User Management (Dashboard Only)

```
users
├── id (primary key)
├── username
├── email
├── password_hash (bcrypt)
├── full_name
├── is_active
├── created_at
└── updated_at

roles (admin, manager, user, viewer)
permissions (leads:view, users:edit, etc.)
user_roles (many-to-many)
role_permissions (many-to-many)
```

### Access Control (Dashboard Only)

```
cities (all 54 Bay Area cities)
agents (all 10 agent types)
user_city_access (restrict which cities)
user_agent_access (restrict which agents)
```

### Tracking (Dashboard Only)

```
sessions (JWT tokens)
audit_logs (user actions)
lead_contacts (who contacted which leads)
```

## Common Queries

### Show all leads found by agents

```sql
SELECT COUNT(*), source
FROM consolidated_leads
GROUP BY source
ORDER BY count DESC;
```

### Show leads a specific user can see

```sql
SELECT l.* FROM consolidated_leads l
WHERE l.city IN (
  SELECT city_id FROM user_city_access
  WHERE user_id = ?
  UNION
  SELECT id FROM cities  -- If no restrictions
  WHERE (SELECT COUNT(*) FROM user_city_access WHERE user_id = ?) = 0
)
AND (
  l.source LIKE (SELECT agent_id FROM user_agent_access WHERE user_id = ?)
  OR (SELECT COUNT(*) FROM user_agent_access WHERE user_id = ?) = 0
);
```

### Track which leads users have contacted

```sql
SELECT u.username, COUNT(lc.id) as contacted_leads
FROM users u
LEFT JOIN lead_contacts lc ON u.id = lc.user_id
GROUP BY u.id
ORDER BY contacted_leads DESC;
```

### Audit: Who contacted which leads

```sql
SELECT u.username, lc.created_at, cl.address, cl.source
FROM lead_contacts lc
JOIN users u ON lc.user_id = u.id
JOIN consolidated_leads cl ON lc.lead_id = cl.id
ORDER BY lc.created_at DESC
LIMIT 100;
```

## Performance Considerations

### Database Indexes

Dashboard automatically creates indexes on:
- `user_roles.user_id`
- `user_city_access.user_id`
- `user_agent_access.user_id`
- `sessions.access_token`
- `audit_logs.user_id`
- `lead_contacts.user_id`

### Scaling Tips

1. **For 50+ users:**
   - Use gunicorn with 4-8 workers
   - Run behind nginx for load balancing
   - Consider read-only replica for dashboard

2. **For millions of leads:**
   - Add indices on `consolidated_leads(city, source, score)`
   - Use pagination (already implemented, 100 per page)
   - Archive old leads (>30 days) to separate table

3. **JWT Token Caching:**
   - Consider Redis for session storage
   - Reduces database queries for token validation

## Troubleshooting Integration

### Dashboard shows no leads

1. Check agents are running and writing to DB
   ```bash
   sqlite3 data/leads.db "SELECT COUNT(*) FROM consolidated_leads;"
   ```

2. Check user has access to cities/agents
   ```sql
   SELECT * FROM user_city_access WHERE user_id = 2;
   SELECT * FROM user_agent_access WHERE user_id = 2;
   ```

3. Check filter parameters aren't too restrictive
   ```bash
   # Try "All Cities" and "All Agents" filters
   ```

### Agents can't write to DB

1. Check database file permissions
   ```bash
   ls -la data/leads.db
   chmod 666 data/leads.db
   ```

2. Check database path in `.env`
   ```bash
   echo $DB_PATH
   ```

### Both systems using different databases

1. Verify `.env` sets `DB_PATH=data/leads.db` for both
2. Start both from the same project root
3. Check no `DB_PATH` env vars are set globally

## Future Enhancements

### Phase 2
- [ ] API for custom lead scoring rules
- [ ] Webhook notifications (Slack, Discord)
- [ ] Lead assignment to team members
- [ ] Commission/bonus tracking
- [ ] Bulk export (CSV, Excel)

### Phase 3
- [ ] Real-time updates via WebSocket
- [ ] Advanced search/filters
- [ ] Custom dashboard widgets
- [ ] Email alerts
- [ ] Mobile app

### Phase 4
- [ ] Machine learning for lead ranking
- [ ] Multi-organization support
- [ ] Advanced analytics
- [ ] Integrations (Pipedrive, HubSpot, Salesforce)

## Summary

The web dashboard integrates seamlessly with the existing lead generation system:

✓ **Shared Database** — Both read/write to same `data/leads.db`  
✓ **No Conflicts** — Agents write leads, dashboard reads/tracks interactions  
✓ **Unified Data** — Same addresses, cities, agents, scores  
✓ **Access Control** — Dashboard adds user management layer on top  
✓ **Independent** — Can run both simultaneously  
✓ **Extensible** — Easy to add new agents/cities/features  

---

For more details, see [DASHBOARD.md](DASHBOARD.md) and [README.md](README.md)
