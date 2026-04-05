# 🚀 Quick Start Guide: Multi-User Dashboard

Get the lead management dashboard running in 5 minutes.

## Prerequisites

- Python 3.11+
- Dependencies already installed (from `requirements.txt`)
- Existing `data/leads.db` with leads from agents (optional, but recommended)

## Step 1: Install Web Dependencies

```bash
pip install -r requirements.txt
```

If you haven't installed yet:
```bash
pip install Flask PyJWT bcrypt flask-cors gunicorn
```

## Step 2: Start the Web Server

```bash
python web_server.py
```

You should see:
```
🚀 Starting Insulleads Web Server on port 5000
   Dashboard: http://localhost:5000/
   Login: http://localhost:5000/login.html
   API: http://localhost:5000/api/
```

The server initializes the database schema on first run (automatically).

## Step 3: Login

Open your browser to: **http://localhost:5000/login.html**

Default credentials:
- **Username:** `admin`
- **Password:** `admin123`

> ⚠️ **Change this password immediately in production!**

## Step 4: View the Dashboard

After login, you'll see:

1. **Stats Bar** — Total leads, new leads, contacted leads
2. **Filters** — City, agent, status, score, value
3. **Leads Table** — All accessible leads with search
4. **Lead Details** — Click any address to view full details

## Step 5: Create Additional Users

Use the demo script to create sample users:

```bash
python web/init_demo_users.py
```

This creates:
- `admin` - Full access
- `manager` - All cities/agents, team management
- `sf_permits` - San Francisco, permits only
- `solar_team` - All cities, solar agent only
- `viewer` - Read-only access

Login as any user to test access controls.

## Manual User Creation via API

Create a user with admin access:

```bash
curl -X POST http://localhost:5000/api/admin/users \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "john",
    "email": "john@example.com",
    "password": "secure123",
    "full_name": "John Doe",
    "roles": ["user"],
    "city_ids": [44, 45],
    "agent_ids": [1, 2]
  }'
```

## Configuration

### Environment Variables

Add to `.env`:

```bash
# Web Server
PORT=5000
FLASK_DEBUG=false
JWT_SECRET_KEY=your-secret-key-here-change-in-production

# Token Expiry
JWT_ACCESS_EXPIRY=3600        # 1 hour
JWT_REFRESH_EXPIRY=604800     # 7 days

# Database (existing)
DB_PATH=data/leads.db
```

### Production Setup

Use gunicorn for production:

```bash
gunicorn -w 4 -b 0.0.0.0:5000 web_server:app
```

Or with custom port:

```bash
PORT=8080 gunicorn -w 4 -b 0.0.0.0:8080 web_server:app
```

### systemd Service

Create `/etc/systemd/system/insulleads-web.service`:

```ini
[Unit]
Description=Insulleads Web Dashboard
After=network.target

[Service]
Type=simple
User=insulleads
WorkingDirectory=/home/insulleads/Insulleads
Environment="JWT_SECRET_KEY=your-secret-here"
ExecStart=/home/insulleads/Insulleads/venv/bin/gunicorn -w 4 -b 0.0.0.0:5000 web_server:app
Restart=always

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl enable insulleads-web
sudo systemctl start insulleads-web
sudo systemctl status insulleads-web
```

## Using the Dashboard

### View Leads

1. **Login** to dashboard
2. **Filter** by city, agent, status, score
3. **Click** on any address to see details
4. **Log Contact** to mark as contacted

### Access Control

Users see:
- Only cities they're assigned to
- Only agents they're assigned to
- Stats filtered by their accessible data
- Their own audit log

### Roles

| Role | What They Can Do |
|------|------------------|
| **admin** | Everything + manage users, roles, permissions |
| **manager** | View all leads, manage team |
| **user** | View assigned leads, log contacts |
| **viewer** | Read-only view of assigned leads |

### Admin Actions

Login as admin to:
1. Create new users
2. Assign cities/agents per user
3. Update permissions
4. View audit logs

## Troubleshooting

### "Permission denied" errors

Check user's assigned cities/agents:

```bash
sqlite3 data/leads.db <<EOF
.mode column
SELECT user_id, city_id FROM user_city_access WHERE user_id = 2;
SELECT user_id, agent_id FROM user_agent_access WHERE user_id = 2;
EOF
```

### Dashboard shows no leads

1. Check agents found leads:
   ```bash
   sqlite3 data/leads.db "SELECT COUNT(*) FROM consolidated_leads;"
   ```

2. Check user has access:
   ```bash
   sqlite3 data/leads.db <<EOF
   SELECT COUNT(*) FROM user_city_access WHERE user_id = 2;
   SELECT COUNT(*) FROM user_agent_access WHERE user_id = 2;
   EOF
   ```

3. Try "All Cities" and "All Agents" filters

### "Invalid token" on login

- Token may have expired
- Clear browser cache (Ctrl+Shift+Delete)
- Try logging in again

### Port already in use

Change port:

```bash
PORT=8080 python web_server.py
```

### Database locked

- Quit any other Python processes
- Check no other instances are running
- Restart the server

## API Examples

### Login and get token

```bash
curl -X POST http://localhost:5000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin123"}'

# Response:
# {
#   "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
#   "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
#   "token_type": "Bearer",
#   "expires_in": 3600
# }
```

### Get current user info

```bash
curl http://localhost:5000/api/user \
  -H "Authorization: Bearer <access-token>"
```

### List leads

```bash
curl "http://localhost:5000/api/leads?city_id=44&agent=permits&min_score=60" \
  -H "Authorization: Bearer <access-token>"
```

### Log contact

```bash
curl -X POST http://localhost:5000/api/leads/lead_123/contact \
  -H "Authorization: Bearer <access-token>" \
  -H "Content-Type: application/json" \
  -d '{"type": "contacted", "notes": "Called about permit"}'
```

## Full Documentation

For complete documentation, see:

- **[DASHBOARD.md](DASHBOARD.md)** — Detailed feature documentation
- **[INTEGRATION.md](INTEGRATION.md)** — How it integrates with lead agents
- **[API Reference](DASHBOARD.md#api-endpoints)** — All API endpoints

## Next Steps

1. ✅ Start web server (`python web_server.py`)
2. ✅ Login to dashboard
3. ✅ Create test users (`python web/init_demo_users.py`)
4. ✅ Test access controls
5. 📖 Read [DASHBOARD.md](DASHBOARD.md) for full features
6. 🔒 Change default admin password
7. 🚀 Deploy to production with gunicorn
8. 🔧 Customize users, roles, permissions

## Support

- Check logs: `tail -f logs/web.log`
- API health: `curl http://localhost:5000/api/health`
- Database: `sqlite3 data/leads.db ".schema"`

---

**You're all set!** 🎉

Dashboard ready at: http://localhost:5000/login.html
