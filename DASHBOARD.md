# 🏠 Insulleads Multi-User Dashboard

Complete multi-user lead management platform with role-based access control, city/agent filtering, and extensible permission system.

## Features

✅ **Multi-User System** — Support for 50+ concurrent users  
✅ **Role-Based Access Control (RBAC)** — 4 predefined roles + custom permissions  
✅ **City-Level Access Control** — Users see only assigned cities  
✅ **Agent-Level Access Control** — Users see only assigned agent types  
✅ **JWT Authentication** — Secure token-based auth with refresh tokens  
✅ **Lead Management** — Filter, search, and track lead interactions  
✅ **Dashboard Stats** — Real-time lead statistics and analytics  
✅ **Audit Logging** — Track all user actions  
✅ **Responsive Design** — Works on desktop and mobile  
✅ **Extensible** — Easy to add new services and cities  

## Architecture

### Database Schema

The dashboard uses the existing `data/leads.db` SQLite database with new tables:

**User Management:**
- `users` — User accounts with password hashing (bcrypt)
- `roles` — Predefined roles (admin, manager, user, viewer)
- `user_roles` — User ↔ Role assignments
- `permissions` — Fine-grained permissions (leads:view, users:edit, etc.)
- `role_permissions` — Role ↔ Permission assignments

**Access Control:**
- `cities` — All 54 Bay Area cities
- `agents` — All 10 agent types (permits, solar, rodents, etc.)
- `user_city_access` — Restrict which cities user can see
- `user_agent_access` — Restrict which agents user can see

**Session & Audit:**
- `sessions` — Active JWT tokens and refresh tokens
- `audit_logs` — User activity tracking
- `lead_contacts` — When users interact with leads

### API Endpoints

**Authentication:**
```
POST   /api/auth/login              Login with username/password
POST   /api/auth/refresh            Refresh access token
POST   /api/auth/logout             Logout and revoke token
```

**Users & Permissions:**
```
GET    /api/user                    Get current user + permissions + accessible cities/agents
POST   /api/admin/users             Create user (admin only)
PUT    /api/admin/users/<id>/access Update user's city/agent access (admin only)
```

**Leads:**
```
GET    /api/leads                   List leads with filters (city, agent, status, score, value)
GET    /api/leads/<id>              Get single lead details
POST   /api/leads/<id>/contact      Log user contact with lead
```

**Dashboard:**
```
GET    /api/stats                   Get user's lead statistics
GET    /api/audit-log               Get user's activity log
```

### Roles & Permissions

**4 Built-in Roles:**

| Role | Permissions |
|------|-------------|
| **admin** | Full access to all features, user management, roles, audit logs |
| **manager** | View all leads, filter by city/agent, manage team members |
| **user** | View leads (filtered by city+agent), contact leads, see stats |
| **viewer** | Read-only access to leads (no contacting) |

**Custom Per-User Permissions:**

Users can be granted/restricted access to:
- Specific cities (leave empty = all cities)
- Specific agents (leave empty = all agents)

Example: User X can see only San Francisco + demolition leads.

## Installation

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Initialize Database

The database schema is created automatically on first run:

```bash
python web_server.py
```

This initializes:
- User/role/permission tables
- All 54 Bay Area cities
- All 10 agents
- Default admin user (credentials: admin/admin123)

### 3. Create Admin User

Default admin user is automatically created:

```
Username: admin
Password: admin123
```

**Change this password immediately in production!**

To create additional users via API (see Admin API section below).

## Running the Web Server

### Development

```bash
export FLASK_DEBUG=true
python web_server.py
```

Server runs on `http://localhost:5000`

### Production (with gunicorn)

```bash
gunicorn -w 4 -b 0.0.0.0:5000 web_server:app
```

Or with custom port:

```bash
PORT=8080 gunicorn -w 4 -b 0.0.0.0:8080 web_server:app
```

### As systemd Service

Create `/etc/systemd/system/insulleads-web.service`:

```ini
[Unit]
Description=Insulleads Web Dashboard
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=insulleads
Group=insulleads
WorkingDirectory=/home/insulleads/Insulleads
Environment="JWT_SECRET_KEY=your-secret-key-here"
ExecStart=/home/insulleads/Insulleads/venv/bin/gunicorn -w 4 -b 0.0.0.0:5000 web_server:app
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl enable insulleads-web
sudo systemctl start insulleads-web
sudo systemctl status insulleads-web
```

## Configuration

Environment variables (in `.env`):

```bash
# Web Server
PORT=5000                          # Flask server port
FLASK_DEBUG=false                  # Debug mode (true/false)
JWT_SECRET_KEY=change-me           # JWT signing key (CHANGE IN PRODUCTION!)
JWT_ACCESS_EXPIRY=3600             # Access token lifetime (seconds, default 1 hour)
JWT_REFRESH_EXPIRY=604800          # Refresh token lifetime (seconds, default 7 days)

# Database (existing)
DB_PATH=data/leads.db              # SQLite database path
```

## Web Dashboard Usage

### Login

1. Navigate to `http://localhost:5000/login.html`
2. Enter username and password
3. Click "Login"

Default credentials: `admin` / `admin123`

### Dashboard Features

**Stats Section:**
- Total leads available to you
- New leads (not yet contacted)
- Contacted leads

**Filters:**
- **City** — Select specific city (or all)
- **Agent** — Select agent type (or all)
- **Status** — All / New / Contacted
- **Min Score** — Filter by lead score (0-100)
- **Min Value** — Filter by estimated value

**Leads Table:**
- Address (clickable to view details)
- City
- Score (color-coded: red=hot, orange=warm, yellow=medium, cyan=cool, gray=cold)
- Source (which agent/API)
- View button
- Contacted indicator (green ✓)

**Lead Details Modal:**
- Full address and location
- Score and estimated value
- Source and description
- Link to original source
- "Log Contact" button to mark as contacted

### Actions

- **View Lead** — Click address or "View" button to see full details
- **Log Contact** — Mark that you've contacted this lead (tracked in audit log)
- **Filter Leads** — Use filters to find specific opportunities
- **Search** — Use address search (in future update)

## Admin API Usage

Create users and manage access via API:

### Create User

```bash
curl -X POST http://localhost:5000/api/admin/users \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "user1",
    "email": "user1@example.com",
    "password": "securepass123",
    "full_name": "User One",
    "roles": ["user"],
    "city_ids": [44, 44],  # San Francisco, San Jose (see cities table)
    "agent_ids": [1, 10]   # permits, deconstruction (see agents table)
  }'
```

### Update User Access

Restrict which cities and agents a user can see:

```bash
curl -X PUT http://localhost:5000/api/admin/users/5/access \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "city_ids": [44],       # Only San Francisco
    "agent_ids": [10]       # Only deconstruction
  }'
```

Leave empty arrays to allow all cities/agents:

```bash
{
  "city_ids": [],    # Can see all cities
  "agent_ids": []    # Can see all agents
}
```

## Database Queries

### Find all 54 cities

```sql
SELECT id, name, county FROM cities ORDER BY name;
```

### Find all agents

```sql
SELECT id, name FROM agents ORDER BY name;
```

### Check user's accessible cities

```sql
SELECT c.id, c.name FROM cities c
WHERE c.id IN (
  SELECT city_id FROM user_city_access WHERE user_id = ?
);
```

### Check user's accessible agents

```sql
SELECT a.id, a.name FROM agents a
WHERE a.id IN (
  SELECT agent_id FROM user_agent_access WHERE user_id = ?
);
```

### View user's activity log

```sql
SELECT action, resource_type, resource_id, created_at
FROM audit_logs
WHERE user_id = ?
ORDER BY created_at DESC
LIMIT 100;
```

## Extensibility

### Adding New Cities

Cities are stored in the `cities` table and automatically populated on startup. To add a new city:

```python
# In web_db.py, add to the cities list:
("New City Name", "CA", "County Name")
```

Then restart the web server.

### Adding New Agents

Agents are auto-populated from `AGENT_REGISTRY` in `main.py`. When you add a new agent to main.py, it's automatically available in:

- User access controls
- Dashboard filters
- API responses

### Adding New Permissions

Define new permissions in `web_db.py`:

```python
("resource_name", "action_name", "Description")
```

Examples:
- `("reports", "view", "View reports")`
- `("export", "leads", "Export leads to CSV")`

Then assign to roles as needed.

### Custom Role Creation

Create new roles in `web_db.py`:

```python
("custom_role_name", "Description of what this role does")
```

And assign permissions via `role_permissions` table.

## Security Considerations

🔒 **Production Checklist:**

- [ ] Change `JWT_SECRET_KEY` to a strong random value
- [ ] Change default admin password
- [ ] Use HTTPS (reverse proxy with nginx/Apache)
- [ ] Set strong database permissions
- [ ] Enable audit logging for compliance
- [ ] Run behind gunicorn (not Flask dev server)
- [ ] Set up rate limiting
- [ ] Keep JWT_ACCESS_EXPIRY short (1 hour)
- [ ] Rotate refresh tokens regularly

### Password Security

- Passwords are hashed with bcrypt (12 rounds)
- Never stored in plain text
- Bcrypt automatically handles salt generation

### Token Security

- Access tokens expire after 1 hour (configurable)
- Refresh tokens expire after 7 days
- Tokens revoked on logout
- Tokens stored in secure HTTP-only cookies (future)

## Troubleshooting

### "Invalid token" on login

- Access token may have expired
- Try refreshing the page and logging in again
- Check that `JWT_SECRET_KEY` is set in `.env`

### "Permission denied" errors

- User may not have permission for that action
- Check user's assigned role in database
- Check user's city/agent access restrictions

### Dashboard won't load

1. Check server is running: `curl http://localhost:5000/api/health`
2. Check browser console for errors (F12)
3. Verify access token in localStorage: `localStorage.getItem('access_token')`
4. Try clearing browser cache and logging in again

### Database locked errors

- Multiple processes may be accessing database
- Check no other instances are running
- Use SQLite with WAL mode for concurrent access (future update)

## API Examples

### Login

```bash
curl -X POST http://localhost:5000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin123"}'

# Response:
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

### Get Current User

```bash
curl -X GET http://localhost:5000/api/user \
  -H "Authorization: Bearer <access_token>"

# Response:
{
  "id": 1,
  "username": "admin",
  "email": "admin@example.com",
  "full_name": "Administrator",
  "roles": ["admin"],
  "permissions": ["audit:view", "leads:contact", "leads:filter", ...],
  "cities": [{id: 1, name: "San Francisco"}, ...],
  "agents": [{id: 1, name: "permits"}, ...]
}
```

### List Leads

```bash
curl -X GET "http://localhost:5000/api/leads?city_id=44&agent=permits&min_score=60&page=1" \
  -H "Authorization: Bearer <access_token>"

# Response:
{
  "leads": [
    {
      "id": "lead_1",
      "address": "123 Main St, San Francisco, CA",
      "city": 44,
      "score": 85,
      "value": 150000,
      "source": "permits_agent",
      "description": "Building permit filed",
      "contacted": false
    }
  ],
  "total": 42,
  "page": 1,
  "per_page": 100,
  "pages": 1
}
```

### Get Dashboard Stats

```bash
curl -X GET http://localhost:5000/api/stats \
  -H "Authorization: Bearer <access_token>"

# Response:
{
  "total_leads": 150,
  "new_leads": 45,
  "contacted_leads": 12,
  "by_agent": {
    "permits_agent": 50,
    "solar_agent": 45,
    "rodents_agent": 55
  },
  "by_city": {
    "San Francisco": 30,
    "Oakland": 28,
    "San Jose": 92
  }
}
```

## Future Enhancements

- [ ] Lead export to CSV
- [ ] Bulk actions (mark multiple as contacted)
- [ ] Advanced search (by address, company name)
- [ ] Custom dashboard widgets
- [ ] Email notifications
- [ ] 2FA/MFA support
- [ ] SAML/LDAP integration
- [ ] Webhook integrations
- [ ] API rate limiting
- [ ] Advanced audit reports
- [ ] Lead assignment to team members
- [ ] Commission tracking

## Support

For issues, check:
1. Server logs: `journalctl -u insulleads-web -f`
2. Browser console: F12
3. Database schema: `sqlite3 data/leads.db ".schema"`
4. API health: `curl http://localhost:5000/api/health`

---

Built for the Insulleads lead generation system. 🚀
