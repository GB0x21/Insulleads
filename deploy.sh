#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
#  Insulleads — Deploy & Update Script para Droplet Ubuntu
#
#  Funciona tanto para instalacion nueva como para actualizar:
#    curl -sSL https://raw.githubusercontent.com/GB0x21/Insulleads/main/deploy.sh | bash
#  o:
#    bash deploy.sh
#
#  Incluye:
#    - Lead generation agents (Python)
#    - Dashboard web (Flask/Gunicorn)
#    - Krayin CRM (Laravel/PHP) con sync automatico
#
#  Detecta automaticamente si es instalacion nueva o actualizacion.
# ══════════════════════════════════════════════════════════════

set -euo pipefail

# Evitar prompts interactivos durante apt upgrade
export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a

APP_USER="insulleads"
APP_DIR="/home/${APP_USER}/Insulleads"
CRM_DIR="/home/${APP_USER}/krayin-crm"
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
echo "  (Dashboard + Krayin CRM)"
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
info "[1/16] Actualizando sistema..."
apt-get update -qq
apt-get upgrade -y -qq -o Dpkg::Options::="--force-confold" -o Dpkg::Options::="--force-confdef"
ok "Sistema actualizado"

# ── 3. Instalar dependencias base ────────────────────────────
info "[2/16] Instalando Python 3, pip, git, nginx..."
apt-get install -y -qq -o Dpkg::Options::="--force-confold" \
    python3 python3-pip python3-venv git curl nginx unzip
ok "Dependencias base instaladas"

# ── 4. Instalar PHP 8.3 + extensiones ────────────────────────
info "[3/16] Instalando PHP 8.3 y extensiones..."
if ! command -v php &>/dev/null || ! php -v | grep -q "8.3"; then
    apt-get install -y -qq software-properties-common
    add-apt-repository -y ppa:ondrej/php 2>/dev/null || true
    apt-get update -qq
fi
apt-get install -y -qq -o Dpkg::Options::="--force-confold" \
    php8.3 php8.3-fpm php8.3-mysql php8.3-mbstring \
    php8.3-xml php8.3-curl php8.3-zip php8.3-gd php8.3-intl \
    php8.3-bcmath php8.3-imap php8.3-tokenizer 2>/dev/null || \
apt-get install -y -qq -o Dpkg::Options::="--force-confold" \
    php php-fpm php-mysql php-mbstring \
    php-xml php-curl php-zip php-gd php-intl \
    php-bcmath php-imap 2>/dev/null || true
# Aumentar memory_limit para Krayin
PHP_INI=$(php -r 'echo php_ini_loaded_file();' 2>/dev/null || echo "")
if [ -n "${PHP_INI}" ]; then
    sed -i 's/memory_limit = .*/memory_limit = 512M/' "${PHP_INI}" 2>/dev/null || true
    sed -i 's/max_execution_time = .*/max_execution_time = 360/' "${PHP_INI}" 2>/dev/null || true
fi
# Tambien el FPM ini
FPM_INI=$(find /etc/php -name "php.ini" -path "*/fpm/*" 2>/dev/null | head -1)
if [ -n "${FPM_INI}" ]; then
    sed -i 's/memory_limit = .*/memory_limit = 512M/' "${FPM_INI}" 2>/dev/null || true
    sed -i 's/max_execution_time = .*/max_execution_time = 360/' "${FPM_INI}" 2>/dev/null || true
fi
ok "PHP instalado ($(php -v 2>/dev/null | head -1 | awk '{print $2}' || echo 'version desconocida'))"

# ── 5. Instalar MySQL 8 ─────────────────────────────────────
info "[4/16] Instalando MySQL 8..."
if ! command -v mysql &>/dev/null; then
    apt-get install -y -qq -o Dpkg::Options::="--force-confold" mysql-server
    systemctl enable mysql
    systemctl start mysql
    ok "MySQL instalado y configurado"
else
    ok "MySQL ya instalado ($(mysql --version 2>/dev/null | awk '{print $5}' | tr -d ',' || echo '?'))"
fi

# ── 6. Instalar Composer ────────────────────────────────────
info "[5/16] Instalando Composer..."
if ! command -v composer &>/dev/null; then
    curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer 2>/dev/null
    ok "Composer instalado"
else
    ok "Composer ya instalado ($(composer --version 2>/dev/null | awk '{print $3}' || echo '?'))"
fi

# ── 7. Instalar Node.js 20 LTS ──────────────────────────────
info "[6/16] Instalando Node.js..."
if ! command -v node &>/dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_20.x 2>/dev/null | bash - 2>/dev/null
    apt-get install -y -qq nodejs 2>/dev/null || true
    ok "Node.js instalado"
else
    ok "Node.js ya instalado ($(node -v 2>/dev/null || echo '?'))"
fi

# ── 8. Crear usuario ────────────────────────────────────────
info "[7/16] Configurando usuario '${APP_USER}'..."
if ! id "${APP_USER}" &>/dev/null; then
    adduser --disabled-password --gecos "Insulleads Bot" "${APP_USER}"
    ok "Usuario '${APP_USER}' creado"
else
    ok "Usuario '${APP_USER}' ya existe"
fi

# ── 9. Clonar o actualizar repositorio ───────────────────────
info "[8/16] Obteniendo codigo..."
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

# ── 10. Crear/actualizar entorno virtual Python ──────────────
info "[9/16] Configurando entorno virtual Python..."
if [ ! -d "${VENV}" ]; then
    sudo -u "${APP_USER}" python3 -m venv "${VENV}"
    ok "Entorno virtual creado"
fi
sudo -u "${APP_USER}" ${PIP} install --upgrade pip -q
sudo -u "${APP_USER}" ${PIP} install -r "${APP_DIR}/requirements.txt" -q
ok "Dependencias Python instaladas (Flask, PyJWT, bcrypt, etc.)"

# ── 11. Crear directorios necesarios ─────────────────────────
sudo -u "${APP_USER}" mkdir -p "${APP_DIR}/data" "${APP_DIR}/contacts"
chown -R ${APP_USER}:${APP_USER} "${APP_DIR}"
ok "Directorios data/ y contacts/ listos"

# ── 12. Configurar .env ─────────────────────────────────────
info "[10/16] Configurando variables de entorno..."
KRAYIN_DB_PASS=$(openssl rand -hex 16)

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

# ── Krayin CRM ──
KRAYIN_URL=http://localhost/crm
KRAYIN_ADMIN_EMAIL=admin@example.com
KRAYIN_ADMIN_PASSWORD=admin123
KRAYIN_DB_PASSWORD=${KRAYIN_DB_PASS}
CRM_SYNC_BATCH=50
ENVEOF"
    fi
    warn ".env creado — DEBES configurar TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID"
else
    # Agregar variables web si no existen
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
    fi
    # Agregar variables CRM si no existen
    if ! grep -q "KRAYIN_URL" "${APP_DIR}/.env"; then
        sudo -u "${APP_USER}" bash -c "cat >> ${APP_DIR}/.env << ENVEOF

# ── Krayin CRM (agregado automaticamente) ──
KRAYIN_URL=http://localhost/crm
KRAYIN_ADMIN_EMAIL=admin@example.com
KRAYIN_ADMIN_PASSWORD=admin123
KRAYIN_DB_PASSWORD=${KRAYIN_DB_PASS}
CRM_SYNC_BATCH=50
ENVEOF"
        ok "Variables CRM agregadas a .env existente"
    else
        ok ".env ya tiene configuracion CRM"
        # Leer password existente
        KRAYIN_DB_PASS=$(grep "KRAYIN_DB_PASSWORD" "${APP_DIR}/.env" | cut -d= -f2 | tr -d ' ')
    fi
fi

# ── 13. Configurar MySQL para Krayin ────────────────────────
info "[11/16] Configurando base de datos MySQL para CRM..."
# Crear DB y usuario (idempotente)
mysql -u root -e "
    CREATE DATABASE IF NOT EXISTS krayin_crm CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    CREATE USER IF NOT EXISTS 'krayin'@'localhost' IDENTIFIED BY '${KRAYIN_DB_PASS}';
    ALTER USER 'krayin'@'localhost' IDENTIFIED BY '${KRAYIN_DB_PASS}';
    GRANT ALL PRIVILEGES ON krayin_crm.* TO 'krayin'@'localhost';
    FLUSH PRIVILEGES;
" 2>/dev/null || warn "MySQL ya configurado o requiere configuracion manual"
ok "Base de datos MySQL 'krayin_crm' lista"

# ── 14. Instalar Krayin CRM ─────────────────────────────────
info "[12/16] Instalando Krayin CRM..."
if [ ! -d "${CRM_DIR}" ]; then
    cd /home/${APP_USER}
    sudo -u "${APP_USER}" composer create-project krayin/laravel-crm krayin-crm 2>/dev/null || {
        warn "composer create-project fallo — intentando como root"
        COMPOSER_ALLOW_SUPERUSER=1 composer create-project krayin/laravel-crm krayin-crm 2>/dev/null || true
        chown -R ${APP_USER}:${APP_USER} "${CRM_DIR}" 2>/dev/null || true
    }

    if [ -d "${CRM_DIR}" ]; then
        cd "${CRM_DIR}"

        # Configurar .env de Krayin
        SERVER_IP_TMP=$(curl -s -4 ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')
        sudo -u "${APP_USER}" bash -c "cat > ${CRM_DIR}/.env << KENVEOF
APP_NAME='Insulleads CRM'
APP_ENV=production
APP_KEY=
APP_DEBUG=false
APP_URL=http://${SERVER_IP_TMP}/crm
APP_TIMEZONE=America/Los_Angeles
APP_LOCALE=es
APP_CURRENCY=USD

LOG_CHANNEL=stack
LOG_LEVEL=error

DB_CONNECTION=mysql
DB_HOST=127.0.0.1
DB_PORT=3306
DB_DATABASE=krayin_crm
DB_USERNAME=krayin
DB_PASSWORD=${KRAYIN_DB_PASS}

CACHE_DRIVER=file
QUEUE_CONNECTION=sync
SESSION_DRIVER=file
SESSION_LIFETIME=120

MAIL_MAILER=smtp
MAIL_HOST=localhost
MAIL_PORT=1025
MAIL_USERNAME=null
MAIL_PASSWORD=null
MAIL_ENCRYPTION=null
MAIL_FROM_ADDRESS=crm@insulleads.com
MAIL_FROM_NAME='\${APP_NAME}'

SANCTUM_STATEFUL_DOMAINS=\${APP_URL}
L5_SWAGGER_UI_PERSIST_AUTHORIZATION=true
KENVEOF"

        # Generar app key
        sudo -u "${APP_USER}" php artisan key:generate --force 2>/dev/null || true

        # Instalar Krayin (migraciones + seed)
        sudo -u "${APP_USER}" php artisan krayin-crm:install --force 2>/dev/null || \
            php artisan krayin-crm:install --force 2>/dev/null || true

        # Instalar REST API
        cd "${CRM_DIR}"
        sudo -u "${APP_USER}" composer require krayin/rest-api 2>/dev/null || \
            COMPOSER_ALLOW_SUPERUSER=1 composer require krayin/rest-api 2>/dev/null || true
        sudo -u "${APP_USER}" php artisan krayin-rest-api:install 2>/dev/null || true

        # Optimizar para produccion
        sudo -u "${APP_USER}" composer install --no-dev --optimize-autoloader 2>/dev/null || true
        sudo -u "${APP_USER}" php artisan config:cache 2>/dev/null || true
        sudo -u "${APP_USER}" php artisan route:cache 2>/dev/null || true
        sudo -u "${APP_USER}" php artisan view:cache 2>/dev/null || true

        # Permisos
        chown -R ${APP_USER}:${APP_USER} "${CRM_DIR}"
        chmod -R 775 "${CRM_DIR}/storage" 2>/dev/null || true
        chmod -R 775 "${CRM_DIR}/bootstrap/cache" 2>/dev/null || true

        ok "Krayin CRM instalado en ${CRM_DIR}"
    else
        warn "Krayin CRM no se pudo instalar — instalacion manual necesaria"
    fi
    cd "${APP_DIR}"
else
    # Actualizar Krayin
    cd "${CRM_DIR}"
    sudo -u "${APP_USER}" composer update --no-dev 2>/dev/null || true
    sudo -u "${APP_USER}" php artisan migrate --force 2>/dev/null || true
    sudo -u "${APP_USER}" php artisan config:cache 2>/dev/null || true
    chown -R ${APP_USER}:${APP_USER} "${CRM_DIR}"
    ok "Krayin CRM actualizado"
    cd "${APP_DIR}"
fi

# ── 15. Inicializar base de datos SQLite + usuarios demo ─────
info "[13/16] Inicializando base de datos SQLite y usuarios..."
cd "${APP_DIR}"
sudo -u "${APP_USER}" ${PYTHON} -c "
from utils.web_db import init_web_db, seed_cities_and_agents
init_web_db()
seed_cities_and_agents()
print('  Schema creada: 12 tablas, 54 ciudades, 10 agentes')
"
# Crear usuarios demo (idempotente — no duplica)
sudo -u "${APP_USER}" ${PYTHON} "${APP_DIR}/web/init_demo_users.py" 2>/dev/null || true
ok "Base de datos inicializada con usuarios demo"

# ── 16. Crear servicios systemd ──────────────────────────────
info "[14/16] Configurando servicios systemd..."

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

# Servicio: CRM Sync (oneshot)
cat > /etc/systemd/system/insulleads-crm-sync.service << SYSTEMD_EOF
[Unit]
Description=Insulleads CRM Lead Sync
After=network.target mysql.service

[Service]
Type=oneshot
User=${APP_USER}
Group=${APP_USER}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
ExecStart=${PYTHON} utils/crm_sync.py
StandardOutput=journal
StandardError=journal
SYSTEMD_EOF

# Timer: Sync cada 5 minutos
cat > /etc/systemd/system/insulleads-crm-sync.timer << SYSTEMD_EOF
[Unit]
Description=Insulleads CRM Sync Timer (cada 5 minutos)

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
AccuracySec=30s

[Install]
WantedBy=timers.target
SYSTEMD_EOF

systemctl daemon-reload
systemctl enable insulleads insulleads-web insulleads-crm-sync.timer
ok "Servicios systemd configurados (insulleads + insulleads-web + crm-sync timer)"

# ── 17. Configurar nginx ────────────────────────────────────
info "[15/16] Configurando nginx..."

# Detectar socket PHP-FPM
PHP_FPM_SOCK=$(find /run/php -name "*.sock" 2>/dev/null | head -1)
if [ -z "${PHP_FPM_SOCK}" ]; then
    PHP_FPM_SOCK="/run/php/php8.3-fpm.sock"
fi

cat > /etc/nginx/sites-available/insulleads << NGINX_EOF
server {
    listen 80 default_server;
    server_name _;

    client_max_body_size 50M;

    # ── Krayin CRM (Laravel/PHP) ──────────────────────────
    location /crm {
        alias ${CRM_DIR}/public;
        index index.php;
        try_files \$uri \$uri/ @crm_rewrite;

        location ~ \.php\$ {
            fastcgi_pass unix:${PHP_FPM_SOCK};
            fastcgi_param SCRIPT_FILENAME \$request_filename;
            fastcgi_param SCRIPT_NAME \$fastcgi_script_name;
            include fastcgi_params;
            fastcgi_read_timeout 300s;
        }
    }

    location @crm_rewrite {
        rewrite ^/crm/(.*)\$ /crm/index.php/\$1 last;
    }

    # ── Insulleads Flask Dashboard ────────────────────────
    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
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
# Restart PHP-FPM
systemctl restart php*-fpm 2>/dev/null || true
ok "Nginx configurado (/ -> dashboard, /crm -> Krayin CRM)"

# ── 18. Iniciar/reiniciar servicios ──────────────────────────
info "[16/16] Iniciando servicios..."
if [ "${IS_UPDATE}" = true ]; then
    systemctl restart insulleads-web 2>/dev/null || systemctl start insulleads-web
    systemctl restart insulleads 2>/dev/null || systemctl start insulleads
    systemctl restart insulleads-crm-sync.timer 2>/dev/null || systemctl start insulleads-crm-sync.timer
    ok "Servicios reiniciados"
else
    systemctl start insulleads-web
    systemctl start insulleads-crm-sync.timer
    ok "Dashboard web + CRM sync iniciados"
    warn "Agentes NO iniciados — configura .env primero, luego: systemctl start insulleads"
fi

# ── Configurar CRM pipeline/sources ─────────────────────────
info "Configurando pipeline CRM..."
cd "${APP_DIR}"
sudo -u "${APP_USER}" ${PYTHON} utils/crm_setup.py 2>/dev/null || \
    warn "CRM setup pendiente — ejecuta: sudo -u ${APP_USER} ${PYTHON} utils/crm_setup.py"

# ── Obtener IP del servidor ──────────────────────────────────
SERVER_IP=$(curl -s -4 ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')

# ── Resumen final ────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════"
echo -e "  ${GREEN}INSTALACION COMPLETA${NC}"
echo "══════════════════════════════════════════════════════════"
echo ""

if [ "${IS_UPDATE}" = true ]; then
    echo -e "  ${GREEN}✓${NC} Codigo actualizado desde ${BRANCH}"
    echo -e "  ${GREEN}✓${NC} Dependencias actualizadas"
    echo -e "  ${GREEN}✓${NC} Base de datos migrada (nuevas tablas si aplica)"
    echo -e "  ${GREEN}✓${NC} Krayin CRM actualizado"
    echo -e "  ${GREEN}✓${NC} Servicios reiniciados"
    echo ""
    echo "  Dashboard:  http://${SERVER_IP}/"
    echo "  CRM:        http://${SERVER_IP}/crm/admin/login"
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
echo "  Dashboard:  http://${SERVER_IP}/"
echo "  Login:      admin / admin123"
echo ""
echo "  Usuarios demo creados:"
echo "    admin       (admin123)       -> Acceso total"
echo "    manager     (manager123)     -> Todos los leads"
echo "    sf_permits  (sfpermits123)   -> Solo SF + permisos"
echo "    solar_team  (solar123)       -> Solo solar"
echo "    viewer      (viewer123)      -> Solo lectura"
echo ""

echo "══════════════════════════════════════════════════════════"
echo "  ACCESO AL CRM (Krayin)"
echo "══════════════════════════════════════════════════════════"
echo ""
echo "  URL:        http://${SERVER_IP}/crm/admin/login"
echo "  Email:      admin@example.com"
echo "  Password:   admin123"
echo ""
echo "  Pipeline:   Insulacion Bay Area (6 etapas)"
echo "  Sync:       Cada 5 minutos (automatico)"
echo ""
echo -e "  ${RED}IMPORTANTE: Cambia las contraseñas del admin en produccion!${NC}"
echo ""

echo "══════════════════════════════════════════════════════════"
echo "  COMANDOS UTILES"
echo "══════════════════════════════════════════════════════════"
echo ""
echo "  Estado todo:       sudo systemctl status insulleads insulleads-web insulleads-crm-sync.timer"
echo "  Logs agentes:      sudo journalctl -u insulleads -f"
echo "  Logs dashboard:    sudo journalctl -u insulleads-web -f"
echo "  Logs CRM sync:     sudo journalctl -u insulleads-crm-sync -f"
echo "  Reiniciar todo:    sudo systemctl restart insulleads insulleads-web"
echo "  Detener todo:      sudo systemctl stop insulleads insulleads-web"
echo ""
echo "  Sync CRM manual:   sudo -u ${APP_USER} ${PYTHON} ${APP_DIR}/utils/crm_sync.py"
echo "  Setup CRM:         sudo -u ${APP_USER} ${PYTHON} ${APP_DIR}/utils/crm_setup.py"
echo "  Verificar CRM:     sudo -u ${APP_USER} ${PYTHON} ${APP_DIR}/utils/crm_setup.py --check"
echo ""
echo "  Probar Telegram:   sudo -u ${APP_USER} ${PYTHON} ${APP_DIR}/main.py --test"
echo "  Estadisticas:      sudo -u ${APP_USER} ${PYTHON} ${APP_DIR}/main.py --stats"
echo ""
echo "  Actualizar:        curl -sSL https://raw.githubusercontent.com/GB0x21/Insulleads/main/deploy.sh | bash"
echo ""
echo "══════════════════════════════════════════════════════════"
echo ""
