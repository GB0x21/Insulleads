#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
#  Lead Generation Agents — Deploy Script para Droplet Ubuntu
#  Ejecutar como root en un droplet recien creado:
#    curl -sSL https://raw.githubusercontent.com/GB0x21/Insulleads/main/deploy.sh | bash
#  o:
#    wget -qO- https://raw.githubusercontent.com/GB0x21/Insulleads/main/deploy.sh | bash
# ══════════════════════════════════════════════════════════════

set -euo pipefail

# Evitar prompts interactivos durante apt upgrade
export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a

APP_USER="insulleads"
APP_DIR="/home/${APP_USER}/Insulleads"
REPO_URL="https://github.com/GB0x21/Insulleads.git"
BRANCH="main"

echo ""
echo "══════════════════════════════════════════════════"
echo "  Lead Generation Agents — Instalacion Automatica"
echo "══════════════════════════════════════════════════"
echo ""

# ── 1. Verificar que estamos como root ─────────────────────
if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: Este script debe ejecutarse como root"
    echo "  sudo bash deploy.sh"
    exit 1
fi

# ── 2. Actualizar sistema ─────────────────────────────────
echo "[1/8] Actualizando sistema..."
apt update -qq && apt upgrade -y -qq -o Dpkg::Options::="--force-confold"

# ── 3. Instalar dependencias ──────────────────────────────
echo "[2/8] Instalando Python 3, pip, git..."
apt install -y -qq -o Dpkg::Options::="--force-confold" python3 python3-pip python3-venv git curl

# ── 4. Crear usuario ──────────────────────────────────────
echo "[3/8] Creando usuario '${APP_USER}'..."
if ! id "${APP_USER}" &>/dev/null; then
    adduser --disabled-password --gecos "Lead Generation Bot" "${APP_USER}"
fi

# ── 5. Clonar repositorio ─────────────────────────────────
echo "[4/8] Clonando repositorio..."
if [ -d "${APP_DIR}" ]; then
    echo "  Directorio ya existe — actualizando..."
    cd "${APP_DIR}"
    sudo -u "${APP_USER}" git pull origin "${BRANCH}" || true
else
    sudo -u "${APP_USER}" git clone -b "${BRANCH}" "${REPO_URL}" "${APP_DIR}"
fi
cd "${APP_DIR}"

# ── 6. Crear entorno virtual ──────────────────────────────
echo "[5/8] Creando entorno virtual Python..."
sudo -u "${APP_USER}" python3 -m venv "${APP_DIR}/venv"
sudo -u "${APP_USER}" "${APP_DIR}/venv/bin/pip" install --upgrade pip -q
sudo -u "${APP_USER}" "${APP_DIR}/venv/bin/pip" install -r "${APP_DIR}/requirements.txt" -q

# ── 7. Crear .env si no existe ─────────────────────────────
echo "[6/8] Configurando .env..."
if [ ! -f "${APP_DIR}/.env" ]; then
    sudo -u "${APP_USER}" cp "${APP_DIR}/.env.example" "${APP_DIR}/.env"
    echo ""
    echo "  ╔═══════════════════════════════════════════════════╗"
    echo "  ║  IMPORTANTE: Edita .env con tus credenciales      ║"
    echo "  ║  nano ${APP_DIR}/.env                             ║"
    echo "  ║                                                   ║"
    echo "  ║  Minimo requerido:                                ║"
    echo "  ║    TELEGRAM_BOT_TOKEN=tu_token_aqui               ║"
    echo "  ║    TELEGRAM_CHAT_ID=tu_chat_id_aqui               ║"
    echo "  ╚═══════════════════════════════════════════════════╝"
    echo ""
else
    echo "  .env ya existe — conservando configuracion actual"
fi

# ── 8. Crear directorios ──────────────────────────────────
sudo -u "${APP_USER}" mkdir -p "${APP_DIR}/data" "${APP_DIR}/contacts"

# ── 9. Crear servicio systemd ──────────────────────────────
echo "[7/8] Configurando servicio systemd..."
cat > /etc/systemd/system/insulleads.service << SYSTEMD_EOF
[Unit]
Description=Lead Generation Agents
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_USER}
WorkingDirectory=${APP_DIR}
ExecStart=${APP_DIR}/venv/bin/python main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# Seguridad
NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=${APP_DIR}/data ${APP_DIR}
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
SYSTEMD_EOF

systemctl daemon-reload
systemctl enable insulleads

# ── 10. Resumen ────────────────────────────────────────────
echo "[8/8] Instalacion completa!"
echo ""
echo "══════════════════════════════════════════════════"
echo "  SIGUIENTE PASO:"
echo "══════════════════════════════════════════════════"
echo ""
echo "  1. Configura Telegram en .env:"
echo "     nano ${APP_DIR}/.env"
echo ""
echo "  2. (Opcional) Copia tus CSVs de contactos:"
echo "     scp *.csv root@TU_IP:${APP_DIR}/contacts/"
echo ""
echo "  3. Prueba la conexion:"
echo "     sudo -u ${APP_USER} ${APP_DIR}/venv/bin/python ${APP_DIR}/main.py --test"
echo ""
echo "  4. Inicia el servicio:"
echo "     sudo systemctl start insulleads"
echo ""
echo "  5. Verifica que esta corriendo:"
echo "     sudo systemctl status insulleads"
echo "     sudo journalctl -u insulleads -f"
echo ""
echo "══════════════════════════════════════════════════"
echo "  COMANDOS UTILES:"
echo "══════════════════════════════════════════════════"
echo ""
echo "  Ver logs:        sudo journalctl -u insulleads -f"
echo "  Reiniciar:       sudo systemctl restart insulleads"
echo "  Detener:         sudo systemctl stop insulleads"
echo "  Estado:          sudo systemctl status insulleads"
echo "  Estadisticas:    sudo -u ${APP_USER} ${APP_DIR}/venv/bin/python ${APP_DIR}/main.py --stats"
echo "  Ejecutar manual: sudo -u ${APP_USER} ${APP_DIR}/venv/bin/python ${APP_DIR}/main.py --run permits"
echo ""
