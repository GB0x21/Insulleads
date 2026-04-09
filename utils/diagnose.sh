#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
#  Insulleads — Diagnostico Completo del Servidor
#
#  Ejecutar como root:
#    bash utils/diagnose.sh
#
#  Detecta automaticamente TODOS los problemas de configuracion.
# ══════════════════════════════════════════════════════════════

set -uo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PASS=0
FAIL=0
WARN=0

ok()   { echo -e "  ${GREEN}[OK]${NC}    $1"; ((PASS++)); }
fail() { echo -e "  ${RED}[FAIL]${NC}  $1"; ((FAIL++)); }
warn() { echo -e "  ${YELLOW}[WARN]${NC}  $1"; ((WARN++)); }
info() { echo -e "  ${CYAN}[INFO]${NC}  $1"; }

APP_DIR="/home/insulleads/Insulleads"
CRM_DIR="/home/insulleads/krayin-crm"
VENV="${APP_DIR}/venv"
PYTHON="${VENV}/bin/python"

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  Insulleads — Diagnostico Completo"
echo "  $(date)"
echo "══════════════════════════════════════════════════════════"

# ── 1. SISTEMA ──────────────────────────────────────────────
echo ""
echo "── 1. Sistema ──────────────────────────────────────────"

# Root check
if [ "$(id -u)" -eq 0 ]; then
    ok "Ejecutando como root"
else
    warn "No es root — algunos checks pueden fallar"
fi

# Disk space
DISK_USE=$(df / | tail -1 | awk '{print $5}' | tr -d '%')
if [ "$DISK_USE" -lt 80 ]; then
    ok "Disco: ${DISK_USE}% usado"
else
    fail "Disco: ${DISK_USE}% usado (>80%)"
fi

# Memory
MEM_AVAIL=$(free -m | awk '/Mem:/{print $7}')
if [ "$MEM_AVAIL" -gt 200 ]; then
    ok "Memoria disponible: ${MEM_AVAIL}MB"
else
    fail "Memoria baja: ${MEM_AVAIL}MB disponible (<200MB)"
fi

# ── 2. DEPENDENCIAS ─────────────────────────────────────────
echo ""
echo "── 2. Dependencias del sistema ─────────────────────────"

for cmd in python3 php mysql nginx composer node sqlite3 curl git; do
    if command -v $cmd &>/dev/null; then
        VER=$($cmd --version 2>/dev/null | head -1 | grep -oP '[\d]+\.[\d]+\.?[\d]*' | head -1)
        ok "$cmd instalado ($VER)"
    else
        fail "$cmd NO instalado"
    fi
done

# PHP extensions
echo ""
echo "── 3. Extensiones PHP ──────────────────────────────────"
for ext in mysql mbstring xml curl zip gd intl bcmath; do
    if php -m 2>/dev/null | grep -qi "$ext"; then
        ok "PHP ext: $ext"
    else
        fail "PHP ext: $ext NO instalado"
    fi
done

# PHP-FPM
if systemctl is-active php*-fpm &>/dev/null; then
    FPM_VER=$(systemctl list-units --type=service | grep php | grep fpm | awk '{print $1}')
    ok "PHP-FPM activo: $FPM_VER"
else
    fail "PHP-FPM NO activo"
fi

# PHP-FPM socket
PHP_SOCK=$(find /run/php -name "*.sock" 2>/dev/null | head -1)
if [ -n "$PHP_SOCK" ]; then
    ok "PHP-FPM socket: $PHP_SOCK"
else
    fail "PHP-FPM socket no encontrado en /run/php/"
fi

# ── 4. SERVICIOS ────────────────────────────────────────────
echo ""
echo "── 4. Servicios ────────────────────────────────────────"

for svc in nginx mysql insulleads; do
    if systemctl is-active "$svc" &>/dev/null; then
        ok "$svc: activo"
    else
        fail "$svc: NO activo"
    fi
done

# Timer
if systemctl is-active insulleads-crm-sync.timer &>/dev/null; then
    NEXT=$(systemctl show insulleads-crm-sync.timer -p NextElapseUSecRealtime --value 2>/dev/null | head -1)
    ok "crm-sync timer: activo (next: $NEXT)"
else
    fail "crm-sync timer: NO activo"
fi

# Flask (should NOT exist)
if systemctl is-active insulleads-web &>/dev/null; then
    warn "insulleads-web (Flask) todavia activo — deberia estar deshabilitado"
else
    ok "insulleads-web (Flask): deshabilitado/inexistente"
fi

# ── 5. USUARIO Y PERMISOS ──────────────────────────────────
echo ""
echo "── 5. Usuario y permisos ───────────────────────────────"

if id insulleads &>/dev/null; then
    ok "Usuario 'insulleads' existe"
else
    fail "Usuario 'insulleads' NO existe"
fi

# Home directory traversable by www-data
HOME_PERM=$(stat -c "%a" /home/insulleads 2>/dev/null)
if [ "$HOME_PERM" = "755" ] || [ "$HOME_PERM" = "751" ]; then
    ok "/home/insulleads permisos: $HOME_PERM (www-data puede traversar)"
else
    fail "/home/insulleads permisos: $HOME_PERM (necesita 755 para www-data)"
fi

# CRM directory
if [ -d "$CRM_DIR" ]; then
    CRM_PERM=$(stat -c "%a" "$CRM_DIR" 2>/dev/null)
    ok "Krayin CRM directorio existe ($CRM_PERM)"
else
    fail "Krayin CRM directorio NO existe: $CRM_DIR"
fi

# Storage writable by www-data
if [ -d "$CRM_DIR/storage" ]; then
    STOR_OWNER=$(stat -c "%U" "$CRM_DIR/storage" 2>/dev/null)
    STOR_PERM=$(stat -c "%a" "$CRM_DIR/storage" 2>/dev/null)
    if [ "$STOR_OWNER" = "www-data" ]; then
        ok "storage/ owner: www-data ($STOR_PERM)"
    else
        fail "storage/ owner: $STOR_OWNER (necesita www-data para PHP-FPM)"
    fi
fi

# Bootstrap/cache writable
if [ -d "$CRM_DIR/bootstrap/cache" ]; then
    BC_OWNER=$(stat -c "%U" "$CRM_DIR/bootstrap/cache" 2>/dev/null)
    if [ "$BC_OWNER" = "www-data" ]; then
        ok "bootstrap/cache/ owner: www-data"
    else
        fail "bootstrap/cache/ owner: $BC_OWNER (necesita www-data)"
    fi
fi

# storage/installed marker
if [ -f "$CRM_DIR/storage/installed" ]; then
    ok "storage/installed marker existe"
else
    fail "storage/installed marker NO existe (causara redirect a /install)"
fi

# ── 6. ARCHIVOS CLAVE ──────────────────────────────────────
echo ""
echo "── 6. Archivos clave ───────────────────────────────────"

for f in \
    "$APP_DIR/main.py" \
    "$APP_DIR/.env" \
    "$APP_DIR/requirements.txt" \
    "$APP_DIR/utils/crm_sync.py" \
    "$APP_DIR/utils/crm_setup.py" \
    "$APP_DIR/utils/dedup.py" \
    "$APP_DIR/data/crm_config.json" \
    "$CRM_DIR/.env" \
    "$CRM_DIR/public/index.php" \
    "$CRM_DIR/artisan" \
    "$VENV/bin/python" \
    "$VENV/bin/pip"; do
    if [ -f "$f" ]; then
        ok "$(echo $f | sed "s|/home/insulleads/||")"
    else
        fail "FALTANTE: $(echo $f | sed "s|/home/insulleads/||")"
    fi
done

# ── 7. PYTHON DEPENDENCIAS ─────────────────────────────────
echo ""
echo "── 7. Python dependencias ──────────────────────────────"

if [ -f "$PYTHON" ]; then
    for pkg in requests dotenv schedule bs4; do
        if $PYTHON -c "import $pkg" 2>/dev/null; then
            ok "Python: $pkg"
        else
            fail "Python: $pkg NO instalado"
        fi
    done
else
    fail "Python venv no existe"
fi

# ── 8. MYSQL / KRAYIN DB ───────────────────────────────────
echo ""
echo "── 8. MySQL / Krayin DB ────────────────────────────────"

if command -v mysql &>/dev/null; then
    # Database exists
    DB_EXISTS=$(mysql -u root -e "SHOW DATABASES LIKE 'krayin_crm'" 2>/dev/null | grep -c krayin_crm)
    if [ "$DB_EXISTS" -gt 0 ]; then
        ok "Database krayin_crm existe"
    else
        fail "Database krayin_crm NO existe"
    fi

    # Key tables
    for tbl in users roles lead_pipelines lead_pipeline_stages lead_sources lead_types leads persons; do
        TBL_EXISTS=$(mysql -u root -e "SELECT COUNT(*) as c FROM information_schema.tables WHERE table_schema='krayin_crm' AND table_name='$tbl'" -sN 2>/dev/null)
        if [ "$TBL_EXISTS" = "1" ]; then
            ROW_COUNT=$(mysql -u root -e "SELECT COUNT(*) FROM krayin_crm.$tbl" -sN 2>/dev/null)
            ok "Tabla $tbl: $ROW_COUNT filas"
        else
            fail "Tabla $tbl NO existe"
        fi
    done

    # Admin user
    ADMIN_COUNT=$(mysql -u root -e "SELECT COUNT(*) FROM krayin_crm.users WHERE email='admin@example.com' AND status=1" -sN 2>/dev/null)
    if [ "$ADMIN_COUNT" = "1" ]; then
        ok "Admin user activo: admin@example.com"
    else
        fail "Admin user NO existe o inactivo"
    fi

    # Pipeline
    PIPELINE=$(mysql -u root -e "SELECT name FROM krayin_crm.lead_pipelines WHERE is_default=1 LIMIT 1" -sN 2>/dev/null)
    if [ -n "$PIPELINE" ]; then
        ok "Pipeline default: $PIPELINE"
    else
        fail "No hay pipeline default"
    fi

    # Stages
    STAGE_COUNT=$(mysql -u root -e "SELECT COUNT(*) FROM krayin_crm.lead_pipeline_stages WHERE lead_pipeline_id=1" -sN 2>/dev/null)
    if [ "$STAGE_COUNT" -ge 4 ]; then
        ok "Pipeline stages: $STAGE_COUNT"
        # List them
        mysql -u root -e "SELECT id, code, name, probability, sort_order FROM krayin_crm.lead_pipeline_stages WHERE lead_pipeline_id=1 ORDER BY sort_order" -t 2>/dev/null | while read line; do
            info "  $line"
        done
    else
        fail "Pipeline stages: $STAGE_COUNT (necesita al menos 4)"
    fi

    # MySQL user krayin
    KRAYIN_USER=$(mysql -u root -e "SELECT COUNT(*) FROM mysql.user WHERE user='krayin' AND host='localhost'" -sN 2>/dev/null)
    if [ "$KRAYIN_USER" = "1" ]; then
        ok "MySQL user 'krayin' existe"
    else
        fail "MySQL user 'krayin' NO existe"
    fi
fi

# ── 9. SQLITE LEADS DB ────────────────────────────────────
echo ""
echo "── 9. SQLite Leads DB ──────────────────────────────────"

SQLITE_DB="$APP_DIR/data/leads.db"
if [ -f "$SQLITE_DB" ]; then
    ok "leads.db existe"

    if command -v sqlite3 &>/dev/null; then
        # Total leads
        TOTAL=$(sqlite3 "$SQLITE_DB" "SELECT COUNT(*) FROM consolidated_leads;" 2>/dev/null)
        ok "Total leads en SQLite: $TOTAL"

        # Unsynced
        UNSYNCED=$(sqlite3 "$SQLITE_DB" "SELECT COUNT(*) FROM consolidated_leads WHERE crm_synced=0;" 2>/dev/null)
        if [ "$UNSYNCED" -gt 0 ]; then
            warn "Leads pendientes de sync: $UNSYNCED"
        else
            ok "Leads pendientes de sync: 0"
        fi

        # Synced
        SYNCED=$(sqlite3 "$SQLITE_DB" "SELECT COUNT(*) FROM consolidated_leads WHERE crm_synced=1;" 2>/dev/null)
        ok "Leads sincronizados: $SYNCED"

        # Has krayin_lead_id column
        HAS_COL=$(sqlite3 "$SQLITE_DB" "PRAGMA table_info(consolidated_leads);" 2>/dev/null | grep -c krayin_lead_id)
        if [ "$HAS_COL" -gt 0 ]; then
            ok "Columna krayin_lead_id existe"
        else
            warn "Columna krayin_lead_id NO existe (se creara en primer sync)"
        fi

        # Sample leads by agent
        info "Leads por agente:"
        sqlite3 "$SQLITE_DB" "SELECT agent_sources, COUNT(*) as cnt FROM consolidated_leads GROUP BY agent_sources ORDER BY cnt DESC LIMIT 10;" 2>/dev/null | while read line; do
            info "  $line"
        done
    else
        fail "sqlite3 no instalado — no se puede inspeccionar DB"
    fi
else
    fail "leads.db NO existe: $SQLITE_DB"
fi

# ── 10. CRM CONFIG ─────────────────────────────────────────
echo ""
echo "── 10. CRM Config (crm_config.json) ────────────────────"

CONFIG_FILE="$APP_DIR/data/crm_config.json"
if [ -f "$CONFIG_FILE" ]; then
    ok "crm_config.json existe"

    # Check pipeline_id
    PID=$(python3 -c "import json; d=json.load(open('$CONFIG_FILE')); print(d.get('pipeline_id',''))" 2>/dev/null)
    if [ -n "$PID" ] && [ "$PID" != "None" ]; then
        ok "pipeline_id: $PID"
    else
        fail "pipeline_id no definido en config"
    fi

    # Check source_ids
    SRC_COUNT=$(python3 -c "import json; d=json.load(open('$CONFIG_FILE')); print(len(d.get('source_ids',{})))" 2>/dev/null)
    if [ "$SRC_COUNT" -gt 0 ]; then
        ok "source_ids: $SRC_COUNT fuentes"
    else
        fail "source_ids vacio — sync no podra asignar fuentes"
    fi

    # Check type_ids
    TYPE_COUNT=$(python3 -c "import json; d=json.load(open('$CONFIG_FILE')); print(len(d.get('type_ids',{})))" 2>/dev/null)
    if [ "$TYPE_COUNT" -gt 0 ]; then
        ok "type_ids: $TYPE_COUNT tipos"
    else
        fail "type_ids vacio"
    fi
else
    fail "crm_config.json NO existe — sync no funcionara"
fi

# ── 11. ENV VARS ───────────────────────────────────────────
echo ""
echo "── 11. Variables de entorno (.env) ─────────────────────"

if [ -f "$APP_DIR/.env" ]; then
    # Krayin URL
    KURL=$(grep "^KRAYIN_URL=" "$APP_DIR/.env" | cut -d= -f2 | tr -d ' ')
    if [ "$KURL" = "http://localhost" ]; then
        ok "KRAYIN_URL=$KURL"
    else
        fail "KRAYIN_URL=$KURL (deberia ser http://localhost)"
    fi

    # Telegram
    TBOT=$(grep "^TELEGRAM_BOT_TOKEN=" "$APP_DIR/.env" | cut -d= -f2 | tr -d ' ')
    if [ -n "$TBOT" ] && [ "$TBOT" != "123456789:ABCdef..." ]; then
        ok "TELEGRAM_BOT_TOKEN configurado"
    else
        warn "TELEGRAM_BOT_TOKEN no configurado (agentes no enviaran alertas)"
    fi

    TCHAT=$(grep "^TELEGRAM_CHAT_ID=" "$APP_DIR/.env" | cut -d= -f2 | tr -d ' ')
    if [ -n "$TCHAT" ] && [ "$TCHAT" != "-1001234567890" ]; then
        ok "TELEGRAM_CHAT_ID configurado"
    else
        warn "TELEGRAM_CHAT_ID no configurado"
    fi

    # DB Path
    DBPATH=$(grep "^DB_PATH=" "$APP_DIR/.env" | cut -d= -f2 | tr -d ' ')
    ok "DB_PATH=$DBPATH"
fi

# Krayin .env
if [ -f "$CRM_DIR/.env" ]; then
    APP_URL=$(grep "^APP_URL=" "$CRM_DIR/.env" | cut -d= -f2 | tr -d ' ')
    ok "Krayin APP_URL=$APP_URL"

    APP_KEY=$(grep "^APP_KEY=" "$CRM_DIR/.env" | cut -d= -f2 | tr -d ' ')
    if [ -n "$APP_KEY" ] && [ "$APP_KEY" != "base64:" ]; then
        ok "Krayin APP_KEY configurado"
    else
        fail "Krayin APP_KEY vacio — ejecuta: php artisan key:generate"
    fi

    DB_PASS=$(grep "^DB_PASSWORD=" "$CRM_DIR/.env" | cut -d= -f2 | tr -d ' ')
    if [ -n "$DB_PASS" ]; then
        ok "Krayin DB_PASSWORD configurado"
    else
        fail "Krayin DB_PASSWORD vacio"
    fi
fi

# ── 12. NGINX ──────────────────────────────────────────────
echo ""
echo "── 12. Nginx ───────────────────────────────────────────"

if nginx -t 2>&1 | grep -q "successful"; then
    ok "Nginx config: sintaxis valida"
else
    fail "Nginx config: ERROR de sintaxis"
    nginx -t 2>&1 | tail -3
fi

# Check root directive
if [ -f /etc/nginx/sites-enabled/insulleads ]; then
    ok "Site insulleads habilitado"
    NGINX_ROOT=$(grep "root " /etc/nginx/sites-enabled/insulleads | head -1 | awk '{print $2}' | tr -d ';')
    if [ "$NGINX_ROOT" = "$CRM_DIR/public" ]; then
        ok "Nginx root: $NGINX_ROOT"
    else
        fail "Nginx root: $NGINX_ROOT (deberia ser $CRM_DIR/public)"
    fi
else
    fail "Site insulleads NO habilitado"
fi

if [ -f /etc/nginx/sites-enabled/default ]; then
    warn "Site 'default' todavia habilitado (puede causar conflicto)"
else
    ok "Site 'default' deshabilitado"
fi

# ── 13. HTTP TESTS ─────────────────────────────────────────
echo ""
echo "── 13. HTTP Tests ──────────────────────────────────────"

# Dashboard
HTTP_DASH=$(curl -s -o /dev/null -w "%{http_code}" http://localhost/admin/dashboard 2>/dev/null)
if [ "$HTTP_DASH" = "200" ]; then
    ok "GET /admin/dashboard: $HTTP_DASH"
elif [ "$HTTP_DASH" = "302" ]; then
    ok "GET /admin/dashboard: $HTTP_DASH (redirect a login — normal si no hay sesion)"
else
    fail "GET /admin/dashboard: $HTTP_DASH"
fi

# Login page
HTTP_LOGIN=$(curl -s -o /dev/null -w "%{http_code}" http://localhost/admin/login 2>/dev/null)
if [ "$HTTP_LOGIN" = "200" ]; then
    ok "GET /admin/login: $HTTP_LOGIN"
else
    fail "GET /admin/login: $HTTP_LOGIN"
fi

# Leads page (may redirect to login)
HTTP_LEADS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost/admin/leads 2>/dev/null)
if [ "$HTTP_LEADS" = "200" ] || [ "$HTTP_LEADS" = "302" ]; then
    ok "GET /admin/leads: $HTTP_LEADS"
else
    fail "GET /admin/leads: $HTTP_LEADS"
fi

# API test
HTTP_API=$(curl -s -o /dev/null -w "%{http_code}" http://localhost/api/v1/admin/login -H "Content-Type: application/json" -d '{"email":"test","password":"test"}' 2>/dev/null)
if [ "$HTTP_API" = "200" ] || [ "$HTTP_API" = "401" ] || [ "$HTTP_API" = "422" ]; then
    ok "POST /api/v1/admin/login: $HTTP_API (REST API funciona)"
else
    fail "POST /api/v1/admin/login: $HTTP_API (REST API no funciona — sync usara MySQL directo)"
fi

# ── 14. REST API PACKAGE ───────────────────────────────────
echo ""
echo "── 14. Krayin REST API Package ─────────────────────────"

if [ -d "$CRM_DIR/vendor/krayin/rest-api" ]; then
    # Check for the known bug
    if grep -q "App.Exceptions.Handler" "$CRM_DIR/vendor/krayin/rest-api/src/Exceptions/Handler.php" 2>/dev/null; then
        fail "REST API tiene bug: App\\Exceptions\\Handler (incompatible con esta version de Laravel)"
    else
        ok "REST API package instalado"
    fi
else
    warn "REST API package NO instalado — sync necesitara MySQL directo"
fi

# ── 15. SYNC DRY RUN ──────────────────────────────────────
echo ""
echo "── 15. CRM Sync Test ───────────────────────────────────"

# Test MySQL connectivity with krayin user
if [ -f "$CRM_DIR/.env" ]; then
    KDB_USER=$(grep "^DB_USERNAME=" "$CRM_DIR/.env" | cut -d= -f2 | tr -d ' ')
    KDB_PASS=$(grep "^DB_PASSWORD=" "$CRM_DIR/.env" | cut -d= -f2 | tr -d ' ')
    KDB_NAME=$(grep "^DB_DATABASE=" "$CRM_DIR/.env" | cut -d= -f2 | tr -d ' ')

    if mysql -u "$KDB_USER" -p"$KDB_PASS" -e "SELECT 1" "$KDB_NAME" &>/dev/null; then
        ok "MySQL conectividad: user=$KDB_USER db=$KDB_NAME"
    else
        fail "MySQL conectividad fallida: user=$KDB_USER db=$KDB_NAME"
    fi
fi

# ── RESUMEN ────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════"
echo -e "  RESUMEN: ${GREEN}${PASS} OK${NC} / ${RED}${FAIL} FAIL${NC} / ${YELLOW}${WARN} WARN${NC}"
echo "══════════════════════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo -e "  ${RED}HAY $FAIL PROBLEMAS QUE NECESITAN ATENCION${NC}"
    echo "  Copia todo el output y pegalo en Claude Code para solucionarlos."
fi

echo ""
