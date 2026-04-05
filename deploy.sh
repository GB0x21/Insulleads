#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
#  Insulleads — Deploy & Update Script para Droplet Ubuntu
#
#  Funciona tanto para instalacion nueva como para actualizar:
#    curl -sSL https://raw.githubusercontent.com/GB0x21/Insulleads/main/deploy.sh | bash
#  o:
#    bash deploy.sh
#
#  Detecta automaticamente si es instalacion nueva o actualizacion.
# ══════════════════════════════════════════════════════════════

set -euo pipefail

# Evitar prompts interactivos durante apt upgrade
export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a

APP_USER="insulleads"
APP_DIR="/home/${APP_USER}/Insulleads"
REPO_URL="https://github.com/GB0x21/Insulleads.git"
BRANCH="main"
VENV="${APP_DIR}/venv"
PIP="${VENV}/bin/pip"
PYTHON="${VENV}/bin/python"

# ── Colores ───────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $1"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
fail()  { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo ""
echo "══════════════════════════════════════════════════════"
echo "  Insulleads — Instalacion / Actualizacion Automatica"
echo "══════════════════════════════════════════════════════"
echo ""

# ── Detectar modo (nueva instalacion vs actualizacion) ────────
IS_UPDATE=false
if [ -d "${APP_DIR}" ] && [ -f "${APP_DIR}/main.py" ]; then
    IS_UPDATE=true
    echo -e "${YELLOW}Modo: ACTUALIZACION${NC} (directorio existente detectado)"
else
    echo -e "${GREEN}Modo: INSTALACION NUEVA${NC}"
fi
echo ""

# ── 1. Verificar root ────────────────────────────────────────
if [ "$(id -u)" -ne 0 ]; then
    fail "Este script debe ejecutarse como root: sudo bash deploy.sh"
fi

# ── 2. Actualizar sistema ────────────────────────────────────
info "[1/10] Actualizando sistema..."
apt-get update -qq
apt-get upgrade -y -qq -o Dpkg::Options::="--force-confold" -o Dpkg::Options::="--force-confdef"
ok "Sistema actualizado"

# ── 3. Instalar dependencias del sistema ──────────────────────
info "[2/10] Instalando Python 3, pip, git, nginx..."
apt-get install -y -qq -o Dpkg::Options::="--force-confold" python3 python3-pip python3-venv git curl nginx
ok "Dependencias del sistema instaladas"

# ── 4. Crear usuario ─────────────────────────────────────────
info "[3/10] Configurando usuario '${APP_USER}'..."
if ! id "${APP_USER}" &>/dev/null; then
    adduser --disabled-password --gecos "Insulleads Bot" "${APP_USER}"
    ok "Usuario '${APP_USER}' creado"
else
    ok "Usuario '${APP_USER}' ya existe"
fi

# ── 5. Clonar o actualizar repositorio ────────────────────────
info "[4/10] Obteniendo codigo..."
if [ "${IS_UPDATE}" = true ]; then
    cd "${APP_DIR}"
    # Guardar cambios locales si existen
    sudo -u "${APP_USER}" git stash 2>/dev/null || true
    sudo -u "${APP_USER}" git pull origin "${BRANCH}" || true
    sudo -u "${APP_USER}" git stash pop 2>/dev/null || true
    ok "Codigo actualizado desde ${BRANCH}"
else
    sudo -u "${APP_USER}" git clone -b "${BRANCH}" "${REPO_URL}" "${APP_DIR}"
    ok "Repositorio clonado"
fi
cd "${APP_DIR}"

# ── 6. Crear/actualizar entorno virtual ───────────────────────
info "[5/10] Configurando entorno virtual Python..."
if [ ! -d "${VENV}" ]; then
    sudo -u "${APP_USER}" python3 -m venv "${VENV}"
    ok "Entorno virtual creado"
fi
sudo -u "${APP_USER}" ${PIP} install --upgrade pip -q
sudo -u "${APP_USER}" ${PIP} install -r "${APP_DIR}/requirements.txt" -q
ok "Dependencias Python instaladas (Flask, PyJWT, bcrypt, etc.)"

# ── 7. Crear directorios necesarios ──────────────────────────
sudo -u "${APP_USER}" mkdir -p "${APP_DIR}/data" "${APP_DIR}/contacts"
chown -R ${APP_USER}:${APP_USER} "${APP_DIR}"
ok "Directorios data/ y contacts/ listos"

# ── 8. Configurar .env ───────────────────────────────────────
info "[6/10] Configurando variables de entorno..."
if [ ! -f "${APP_DIR}/.env" ]; then
    if [ -f "${APP_DIR}/.env.example" ]; then
        sudo -u "${APP_USER}" cp "${APP_DIR}/.env.example" "${APP_DIR}/.env"
    else
        # Crear .env minimo
        JWT_KEY=$(openssl rand -hex 32)
        sudo -u "${APP_USER}" bash -c "cat > ${APP_DIR}/.env << ENVEOF
# ── Telegram (REQUERIDO) ──
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# ── Dashboard Web ──
JWT_SECRET_KEY=${JWT_KEY}
JWT_ACCESS_EXPIRY=3600
JWT_REFRESH_EXPIRY=604800
PORT=5000

# ── Base de datos ──
DB_PATH=data/leads.db
ENVEOF"
    fi
    warn ".env creado — DEBES configurar TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID"
else
    # Agregar variables web si no existen en .env existente
    if ! grep -q "JWT_SECRET_KEY" "${APP_DIR}/.env"; then
        JWT_KEY=$(openssl rand -hex 32)
        sudo -u "${APP_USER}" bash -c "cat >> ${APP_DIR}/.env << ENVEOF

# ── Dashboard Web (agregado automaticamente) ──
JWT_SECRET_KEY=${JWT_KEY}
JWT_ACCESS_EXPIRY=3600
JWT_REFRESH_EXPIRY=604800
PORT=5000
ENVEOF"
        ok "Variables JWT agregadas a .env existente"
    else
        ok ".env ya tiene configuracion web"
    fi
fi

# ── 9. Inicializar base de datos + usuarios demo ─────────────
info "[7/10] Inicializando base de datos y usuarios..."
sudo -u "${APP_USER}" ${PYTHON} -c "
from utils.web_db import init_web_db, seed_cities_and_agents
init_web_db()
seed_cities_and_agents()
print('  Schema creada: 12 tablas, 54 ciudades, 10 agentes')
"
# Crear usuarios demo (idempotente — no duplica)
sudo -u "${APP_USER}" ${PYTHON} "${APP_DIR}/web/init_demo_users.py" 2>/dev/null || true
ok "Base de datos inicializada con usuarios demo"

# ── 10. Crear servicios systemd ───────────────────────────────
info "[8/10] Configurando servicios systemd..."

# Servicio: Agentes de leads
cat > /etc/systemd/system/insulleads.service << SYSTEMD_EOF
[Unit]
Description=Insulleads Lead Generation Agents
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
ExecStart=${PYTHON} main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=${APP_DIR}/data ${APP_DIR}
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
SYSTEMD_EOF

# Servicio: Dashboard web
cat > /etc/systemd/system/insulleads-web.service << SYSTEMD_EOF
[Unit]
Description=Insulleads Web Dashboard
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
ExecStart=${VENV}/bin/gunicorn -w 4 -b 127.0.0.1:5000 web_server:app
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=${APP_DIR}/data ${APP_DIR}
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
SYSTEMD_EOF

systemctl daemon-reload
systemctl enable insulleads insulleads-web
ok "Servicios systemd configurados (insulleads + insulleads-web)"

# ── 11. Configurar nginx como reverse proxy ───────────────────
info "[9/10] Configurando nginx..."
cat > /etc/nginx/sites-available/insulleads << 'NGINX_EOF'
server {
    listen 80 default_server;
    server_name _;

    # Dashboard web
    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 30s;
        proxy_connect_timeout 10s;
    }
}
NGINX_EOF

# Activar config y desactivar default
ln -sf /etc/nginx/sites-available/insulleads /etc/nginx/sites-enabled/insulleads
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl restart nginx
systemctl enable nginx
ok "Nginx configurado como reverse proxy (puerto 80 -> dashboard)"

# ── 12. Iniciar/reiniciar servicios ───────────────────────────
info "[10/10] Iniciando servicios..."
if [ "${IS_UPDATE}" = true ]; then
    systemctl restart insulleads-web 2>/dev/null || systemctl start insulleads-web
    systemctl restart insulleads 2>/dev/null || systemctl start insulleads
    ok "Servicios reiniciados"
else
    systemctl start insulleads-web
    ok "Dashboard web iniciado"
    warn "Agentes NO iniciados — configura .env primero, luego: systemctl start insulleads"
fi

# ── Obtener IP del servidor ───────────────────────────────────
SERVER_IP=$(curl -s -4 ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')

# ── Resumen final ─────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════"
echo -e "  ${GREEN}INSTALACION COMPLETA${NC}"
echo "══════════════════════════════════════════════════════════"
echo ""

if [ "${IS_UPDATE}" = true ]; then
    echo -e "  ${GREEN}✓${NC} Codigo actualizado desde ${BRANCH}"
    echo -e "  ${GREEN}✓${NC} Dependencias actualizadas"
    echo -e "  ${GREEN}✓${NC} Base de datos migrada (nuevas tablas si aplica)"
    echo -e "  ${GREEN}✓${NC} Servicios reiniciados"
    echo ""
    echo "  Dashboard: http://${SERVER_IP}/"
    echo ""
else
    echo "  SIGUIENTE PASO — Configura Telegram:"
    echo ""
    echo "    nano ${APP_DIR}/.env"
    echo ""
    echo "    TELEGRAM_BOT_TOKEN=tu_token_aqui"
    echo "    TELEGRAM_CHAT_ID=tu_chat_id_aqui"
    echo ""
    echo "  Luego inicia los agentes:"
    echo ""
    echo "    sudo systemctl start insulleads"
    echo ""
fi

echo "══════════════════════════════════════════════════════════"
echo "  ACCESO AL DASHBOARD"
echo "══════════════════════════════════════════════════════════"
echo ""
echo "  URL:      http://${SERVER_IP}/"
echo "  Login:    admin / admin123"
echo ""
echo "  Usuarios demo creados:"
echo "    admin       (admin123)       -> Acceso total"
echo "    manager     (manager123)     -> Todos los leads"
echo "    sf_permits  (sfpermits123)   -> Solo SF + permisos"
echo "    solar_team  (solar123)       -> Solo solar"
echo "    viewer      (viewer123)      -> Solo lectura"
echo ""
echo -e "  ${RED}IMPORTANTE: Cambia la contraseña del admin en produccion!${NC}"
echo ""
echo "══════════════════════════════════════════════════════════"
echo "  COMANDOS UTILES"
echo "══════════════════════════════════════════════════════════"
echo ""
echo "  Estado:          sudo systemctl status insulleads insulleads-web"
echo "  Logs agentes:    sudo journalctl -u insulleads -f"
echo "  Logs dashboard:  sudo journalctl -u insulleads-web -f"
echo "  Reiniciar todo:  sudo systemctl restart insulleads insulleads-web"
echo "  Detener todo:    sudo systemctl stop insulleads insulleads-web"
echo ""
echo "  Probar Telegram: sudo -u ${APP_USER} ${PYTHON} ${APP_DIR}/main.py --test"
echo "  Estadisticas:    sudo -u ${APP_USER} ${PYTHON} ${APP_DIR}/main.py --stats"
echo ""
echo "  Actualizar:      curl -sSL https://raw.githubusercontent.com/GB0x21/Insulleads/main/deploy.sh | bash"
echo ""
echo "══════════════════════════════════════════════════════════"
echo ""
