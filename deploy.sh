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
#    - Krayin CRM (Laravel/PHP) con sync automatico
#
#  Detecta automaticamente si es instalacion nueva o actualizacion.
# ══════════════════════════════════════════════════════════════

set -euo pipefail

# Evitar prompts interactivos durante apt upgrade
export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a
export COMPOSER_ALLOW_SUPERUSER=1

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
echo "  (Agentes + Krayin CRM)"
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
ok "Dependencias Python instaladas"

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
        sudo -u "${APP_USER}" bash -c "cat > ${APP_DIR}/.env << ENVEOF
# ── Telegram (REQUERIDO) ──
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# ── Base de datos ──
DB_PATH=data/leads.db

# ── Krayin CRM ──
KRAYIN_URL=http://localhost
KRAYIN_ADMIN_EMAIL=admin@example.com
KRAYIN_ADMIN_PASSWORD=admin123
KRAYIN_DB_PASSWORD=${KRAYIN_DB_PASS}
CRM_SYNC_BATCH=50
ENVEOF"
    fi
    warn ".env creado — DEBES configurar TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID"
else
    # Agregar variables CRM si no existen
    if ! grep -q "KRAYIN_URL" "${APP_DIR}/.env"; then
        sudo -u "${APP_USER}" bash -c "cat >> ${APP_DIR}/.env << ENVEOF

# ── Krayin CRM (agregado automaticamente) ──
KRAYIN_URL=http://localhost
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
APP_URL=http://${SERVER_IP_TMP}
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

        # Mark as installed to prevent redirect to /install
        touch "${CRM_DIR}/storage/installed"
        chown ${APP_USER}:${APP_USER} "${CRM_DIR}/storage/installed"

        # Instalar REST API
        cd "${CRM_DIR}"
        sudo -u "${APP_USER}" composer require krayin/rest-api 2>/dev/null || \
            COMPOSER_ALLOW_SUPERUSER=1 composer require krayin/rest-api 2>/dev/null || true
        sudo -u "${APP_USER}" php artisan krayin-rest-api:install 2>/dev/null || true

        # Create admin user if not exists (fallback — krayin-crm:install sometimes skips this)
        info "Creando usuario admin del CRM..."
        ADMIN_EMAIL_CRM=$(grep "KRAYIN_ADMIN_EMAIL" "${APP_DIR}/.env" 2>/dev/null | cut -d= -f2 | tr -d ' ')
        ADMIN_PASS_CRM=$(grep "KRAYIN_ADMIN_PASSWORD" "${APP_DIR}/.env" 2>/dev/null | cut -d= -f2 | tr -d ' ')
        ADMIN_EMAIL_CRM=${ADMIN_EMAIL_CRM:-admin@example.com}
        ADMIN_PASS_CRM=${ADMIN_PASS_CRM:-admin123}

        # Ensure role exists
        mysql -u root -e "
            INSERT IGNORE INTO krayin_crm.roles (id, name, description, permission_type, created_at, updated_at)
            VALUES (1, 'Administrator', 'Full access', 'all', NOW(), NOW());
        " 2>/dev/null || true

        # Create admin user via PHP (proper bcrypt hashing)
        HASHED_PASS=$(php -r "echo password_hash('${ADMIN_PASS_CRM}', PASSWORD_BCRYPT);")
        mysql -u root -e "
            INSERT INTO krayin_crm.users (name, email, password, status, role_id, created_at, updated_at)
            VALUES ('Admin', '${ADMIN_EMAIL_CRM}', '${HASHED_PASS}', 1, 1, NOW(), NOW())
            ON DUPLICATE KEY UPDATE password='${HASHED_PASS}', status=1;
        " 2>/dev/null && ok "Admin user creado: ${ADMIN_EMAIL_CRM}" || \
            warn "Admin user ya existe o requiere creacion manual"

        # Optimizar para produccion
        sudo -u "${APP_USER}" composer install --no-dev --optimize-autoloader 2>/dev/null || true

        # Clear all caches first (prevent stale routes/config)
        sudo -u "${APP_USER}" php artisan cache:clear 2>/dev/null || true
        sudo -u "${APP_USER}" php artisan config:clear 2>/dev/null || true
        sudo -u "${APP_USER}" php artisan route:clear 2>/dev/null || true
        sudo -u "${APP_USER}" php artisan view:clear 2>/dev/null || true

        # Rebuild caches
        sudo -u "${APP_USER}" php artisan config:cache 2>/dev/null || true
        sudo -u "${APP_USER}" php artisan route:cache 2>/dev/null || true
        sudo -u "${APP_USER}" php artisan view:cache 2>/dev/null || true

        # Permisos — owner is app user, but www-data (PHP-FPM) needs storage write access
        chown -R ${APP_USER}:${APP_USER} "${CRM_DIR}"
        chown -R www-data:www-data "${CRM_DIR}/storage" 2>/dev/null || true
        chown -R www-data:www-data "${CRM_DIR}/bootstrap/cache" 2>/dev/null || true
        chmod -R 775 "${CRM_DIR}/storage" 2>/dev/null || true
        chmod -R 775 "${CRM_DIR}/bootstrap/cache" 2>/dev/null || true

        # Ensure www-data can traverse to CRM directory
        chmod 755 /home/${APP_USER}
        chmod 755 "${CRM_DIR}"

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

    # Clear + rebuild caches
    sudo -u "${APP_USER}" php artisan cache:clear 2>/dev/null || true
    sudo -u "${APP_USER}" php artisan config:clear 2>/dev/null || true
    sudo -u "${APP_USER}" php artisan route:clear 2>/dev/null || true
    sudo -u "${APP_USER}" php artisan config:cache 2>/dev/null || true
    sudo -u "${APP_USER}" php artisan route:cache 2>/dev/null || true
    sudo -u "${APP_USER}" php artisan view:cache 2>/dev/null || true

    # Ensure installed marker exists
    touch "${CRM_DIR}/storage/installed"

    # Ensure admin user exists
    ADMIN_EMAIL_CRM=$(grep "KRAYIN_ADMIN_EMAIL" "${APP_DIR}/.env" 2>/dev/null | cut -d= -f2 | tr -d ' ')
    ADMIN_PASS_CRM=$(grep "KRAYIN_ADMIN_PASSWORD" "${APP_DIR}/.env" 2>/dev/null | cut -d= -f2 | tr -d ' ')
    ADMIN_EMAIL_CRM=${ADMIN_EMAIL_CRM:-admin@example.com}
    ADMIN_PASS_CRM=${ADMIN_PASS_CRM:-admin123}
    HASHED_PASS=$(php -r "echo password_hash('${ADMIN_PASS_CRM}', PASSWORD_BCRYPT);")
    mysql -u root -e "
        INSERT IGNORE INTO krayin_crm.roles (id, name, description, permission_type, created_at, updated_at)
        VALUES (1, 'Administrator', 'Full access', 'all', NOW(), NOW());
    " 2>/dev/null || true
    mysql -u root -e "
        INSERT INTO krayin_crm.users (name, email, password, status, role_id, created_at, updated_at)
        VALUES ('Admin', '${ADMIN_EMAIL_CRM}', '${HASHED_PASS}', 1, 1, NOW(), NOW())
        ON DUPLICATE KEY UPDATE password='${HASHED_PASS}', status=1;
    " 2>/dev/null || true

    # Fix permissions for PHP-FPM (www-data)
    chown -R ${APP_USER}:${APP_USER} "${CRM_DIR}"
    chown -R www-data:www-data "${CRM_DIR}/storage" 2>/dev/null || true
    chown -R www-data:www-data "${CRM_DIR}/bootstrap/cache" 2>/dev/null || true
    chmod -R 775 "${CRM_DIR}/storage" 2>/dev/null || true
    chmod -R 775 "${CRM_DIR}/bootstrap/cache" 2>/dev/null || true
    chmod 755 /home/${APP_USER}
    chmod 755 "${CRM_DIR}"

    ok "Krayin CRM actualizado"
    cd "${APP_DIR}"
fi

# ── 15. Crear servicios systemd ──────────────────────────────
info "[13/16] Configurando servicios systemd..."

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
systemctl enable insulleads insulleads-crm-sync.timer
# Detener y deshabilitar el servicio Flask si existe de una instalacion anterior
systemctl stop insulleads-web 2>/dev/null || true
systemctl disable insulleads-web 2>/dev/null || true
ok "Servicios systemd configurados (insulleads + crm-sync timer)"

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

    # ── Krayin CRM (pagina principal) ─────────────────────
    root ${CRM_DIR}/public;
    index index.php;

    # ── PHP handler ───────────────────────────────────────
    location ~ \.php\$ {
        fastcgi_pass unix:${PHP_FPM_SOCK};
        include fastcgi_params;
        fastcgi_param SCRIPT_FILENAME \$document_root\$fastcgi_script_name;
        fastcgi_read_timeout 300s;
    }

    # ── Laravel routing (catch-all) ───────────────────────
    location / {
        try_files \$uri \$uri/ /index.php?\$query_string;
    }

    # ── Denegar acceso a archivos ocultos ─────────────────
    location ~ /\.(?!well-known) {
        deny all;
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
ok "Nginx configurado (/ -> Krayin CRM)"

# ── 18. Iniciar/reiniciar servicios ──────────────────────────
info "[14/16] Iniciando servicios..."
if [ "${IS_UPDATE}" = true ]; then
    systemctl restart insulleads 2>/dev/null || systemctl start insulleads
    systemctl restart insulleads-crm-sync.timer 2>/dev/null || systemctl start insulleads-crm-sync.timer
    ok "Servicios reiniciados"
else
    systemctl start insulleads-crm-sync.timer
    ok "CRM sync iniciado"
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
    echo -e "  ${GREEN}✓${NC} Krayin CRM actualizado"
    echo -e "  ${GREEN}✓${NC} Servicios reiniciados"
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
fi

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  ACCESO AL CRM"
echo "══════════════════════════════════════════════════════════"
echo ""
echo "  URL:        http://${SERVER_IP}/admin/login"
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
echo "  Estado:            sudo systemctl status insulleads insulleads-crm-sync.timer"
echo "  Logs agentes:      sudo journalctl -u insulleads -f"
echo "  Logs CRM sync:     sudo journalctl -u insulleads-crm-sync -f"
echo "  Reiniciar:         sudo systemctl restart insulleads"
echo "  Detener:           sudo systemctl stop insulleads"
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
