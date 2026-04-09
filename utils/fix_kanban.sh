#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
#  fix_kanban.sh — Fix Krayin CRM Kanban view issues
#
#  Fixes:
#    1. Pipeline rotten_days (prevents false rotten indicators)
#    2. Stage sort_order (required for Kanban column ordering)
#    3. Leads stage assignment (all active → Nuevo)
#    4. Leads user_id (required for Kanban display)
#    5. Asset cache clear & republish
#    6. Service restart
#
#  Usage:
#    sudo bash utils/fix_kanban.sh
# ══════════════════════════════════════════════════════════════

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $1"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
fail()  { echo -e "${RED}[ERROR]${NC} $1"; }

APP_USER="insulleads"
CRM_DIR="/home/${APP_USER}/krayin-crm"

echo ""
echo "══════════════════════════════════════════════════════"
echo "  Fix Krayin CRM Kanban View"
echo "══════════════════════════════════════════════════════"
echo ""

# ── Verify root ──────────────────────────────────────────────
if [ "$(id -u)" -ne 0 ]; then
    fail "Ejecutar como root: sudo bash utils/fix_kanban.sh"
    exit 1
fi

# ── 1. Fix pipeline configuration ───────────────────────────
info "[1/8] Fijando configuracion del pipeline..."

# Get pipeline ID
PIPELINE_ID=$(mysql -u root -sN -e "SELECT id FROM krayin_crm.lead_pipelines LIMIT 1;" 2>/dev/null)
if [ -z "${PIPELINE_ID}" ]; then
    fail "No se encontro pipeline. Ejecuta deploy.sh primero."
    exit 1
fi

# Set rotten_days to 30 (prevents all leads showing as rotten/red)
mysql -u root -e "
    UPDATE krayin_crm.lead_pipelines
    SET rotten_days = 30, is_default = 1
    WHERE id = ${PIPELINE_ID};
" 2>/dev/null
ok "Pipeline #${PIPELINE_ID}: rotten_days=30, is_default=1"

# ── 2. Fix stage sort_order ──────────────────────────────────
info "[2/8] Fijando sort_order de stages..."

# Get stages for this pipeline
STAGES=$(mysql -u root -sN -e "
    SELECT id, code, sort_order FROM krayin_crm.lead_pipeline_stages
    WHERE lead_pipeline_id = ${PIPELINE_ID}
    ORDER BY id;
" 2>/dev/null)

if [ -z "${STAGES}" ]; then
    warn "No se encontraron stages. Creandolos..."
    mysql -u root -e "
        INSERT INTO krayin_crm.lead_pipeline_stages (name, code, probability, sort_order, lead_pipeline_id)
        VALUES
            ('Nuevo',      'nuevo',      0,   1, ${PIPELINE_ID}),
            ('Contactado', 'contactado', 20,  2, ${PIPELINE_ID}),
            ('Calificado', 'calificado', 40,  3, ${PIPELINE_ID}),
            ('Propuesta',  'propuesta',  60,  4, ${PIPELINE_ID}),
            ('Ganado',     'won',        100, 5, ${PIPELINE_ID}),
            ('Perdido',    'lost',       0,   6, ${PIPELINE_ID});
    " 2>/dev/null
    ok "Stages creados con sort_order correcto"
else
    # Fix sort_order based on stage code
    mysql -u root -e "
        UPDATE krayin_crm.lead_pipeline_stages SET sort_order = 1, probability = 0
        WHERE lead_pipeline_id = ${PIPELINE_ID} AND code = 'nuevo';

        UPDATE krayin_crm.lead_pipeline_stages SET sort_order = 2, probability = 20
        WHERE lead_pipeline_id = ${PIPELINE_ID} AND code = 'contactado';

        UPDATE krayin_crm.lead_pipeline_stages SET sort_order = 3, probability = 40
        WHERE lead_pipeline_id = ${PIPELINE_ID} AND code = 'calificado';

        UPDATE krayin_crm.lead_pipeline_stages SET sort_order = 4, probability = 60
        WHERE lead_pipeline_id = ${PIPELINE_ID} AND code = 'propuesta';

        UPDATE krayin_crm.lead_pipeline_stages SET sort_order = 5, probability = 100
        WHERE lead_pipeline_id = ${PIPELINE_ID} AND code = 'won';

        UPDATE krayin_crm.lead_pipeline_stages SET sort_order = 6, probability = 0
        WHERE lead_pipeline_id = ${PIPELINE_ID} AND code = 'lost';
    " 2>/dev/null
    ok "Stages sort_order actualizado"
fi

# ── 3. Get stage IDs ─────────────────────────────────────────
info "[3/8] Obteniendo IDs de stages..."

NUEVO_ID=$(mysql -u root -sN -e "
    SELECT id FROM krayin_crm.lead_pipeline_stages
    WHERE lead_pipeline_id = ${PIPELINE_ID} AND code = 'nuevo'
    LIMIT 1;
" 2>/dev/null)

if [ -z "${NUEVO_ID}" ]; then
    # Fallback: try 'new' code or just get the first stage
    NUEVO_ID=$(mysql -u root -sN -e "
        SELECT id FROM krayin_crm.lead_pipeline_stages
        WHERE lead_pipeline_id = ${PIPELINE_ID}
        ORDER BY sort_order ASC
        LIMIT 1;
    " 2>/dev/null)
fi

if [ -z "${NUEVO_ID}" ]; then
    fail "No se pudo encontrar el stage 'Nuevo'"
    exit 1
fi

ok "Stage 'Nuevo' ID: ${NUEVO_ID}"

# Show all stages
echo ""
mysql -u root -e "
    SELECT id, name, code, sort_order, probability
    FROM krayin_crm.lead_pipeline_stages
    WHERE lead_pipeline_id = ${PIPELINE_ID}
    ORDER BY sort_order;
" 2>/dev/null
echo ""

# ── 4. Fix leads — all active leads to Nuevo ─────────────────
info "[4/8] Moviendo todos los leads activos a 'Nuevo'..."

LEAD_COUNT=$(mysql -u root -sN -e "SELECT COUNT(*) FROM krayin_crm.leads WHERE status = 1;" 2>/dev/null)
info "  Leads activos encontrados: ${LEAD_COUNT}"

mysql -u root -e "
    UPDATE krayin_crm.leads
    SET lead_pipeline_stage_id = ${NUEVO_ID},
        lead_pipeline_id = ${PIPELINE_ID}
    WHERE status = 1;
" 2>/dev/null
ok "Todos los leads activos movidos a stage 'Nuevo' (${NUEVO_ID})"

# ── 5. Fix leads — ensure user_id is set ─────────────────────
info "[5/8] Fijando user_id en todos los leads..."

ADMIN_ID=$(mysql -u root -sN -e "SELECT id FROM krayin_crm.users ORDER BY id LIMIT 1;" 2>/dev/null)
ADMIN_ID=${ADMIN_ID:-1}

mysql -u root -e "
    UPDATE krayin_crm.leads
    SET user_id = ${ADMIN_ID}
    WHERE user_id IS NULL;
" 2>/dev/null
ok "user_id = ${ADMIN_ID} asignado a todos los leads sin usuario"

# ── 6. Verify lead data integrity ────────────────────────────
info "[6/8] Verificando integridad de datos..."

# Check for leads with invalid stage IDs
INVALID_STAGE=$(mysql -u root -sN -e "
    SELECT COUNT(*) FROM krayin_crm.leads l
    WHERE l.status = 1
    AND l.lead_pipeline_stage_id NOT IN (
        SELECT id FROM krayin_crm.lead_pipeline_stages
        WHERE lead_pipeline_id = ${PIPELINE_ID}
    );
" 2>/dev/null)

if [ "${INVALID_STAGE}" != "0" ] && [ -n "${INVALID_STAGE}" ]; then
    warn "${INVALID_STAGE} leads con stage_id invalido — fijando..."
    mysql -u root -e "
        UPDATE krayin_crm.leads
        SET lead_pipeline_stage_id = ${NUEVO_ID}
        WHERE status = 1
        AND lead_pipeline_stage_id NOT IN (
            SELECT id FROM krayin_crm.lead_pipeline_stages
            WHERE lead_pipeline_id = ${PIPELINE_ID}
        );
    " 2>/dev/null
fi

# Check for leads with NULL expected_close_date
mysql -u root -e "
    UPDATE krayin_crm.leads
    SET expected_close_date = DATE_ADD(CURDATE(), INTERVAL 30 DAY)
    WHERE expected_close_date IS NULL AND status = 1;
" 2>/dev/null

# Check for leads with NULL created_at
mysql -u root -e "
    UPDATE krayin_crm.leads
    SET created_at = NOW()
    WHERE created_at IS NULL;
" 2>/dev/null

# Summary
echo ""
mysql -u root -e "
    SELECT
        s.name AS stage,
        s.sort_order,
        COUNT(l.id) AS total_leads
    FROM krayin_crm.lead_pipeline_stages s
    LEFT JOIN krayin_crm.leads l ON l.lead_pipeline_stage_id = s.id AND l.status = 1
    WHERE s.lead_pipeline_id = ${PIPELINE_ID}
    GROUP BY s.id, s.name, s.sort_order
    ORDER BY s.sort_order;
" 2>/dev/null
echo ""
ok "Datos verificados y corregidos"

# ── 7. Clear and rebuild Laravel caches ──────────────────────
info "[7/8] Limpiando caches de Laravel..."

cd "${CRM_DIR}"

# Give app user ownership for artisan commands
chown -R ${APP_USER}:${APP_USER} "${CRM_DIR}"

# Clear ALL caches
sudo -u "${APP_USER}" php artisan cache:clear 2>/dev/null || true
sudo -u "${APP_USER}" php artisan config:clear 2>/dev/null || true
sudo -u "${APP_USER}" php artisan route:clear 2>/dev/null || true
sudo -u "${APP_USER}" php artisan view:clear 2>/dev/null || true
sudo -u "${APP_USER}" php artisan event:clear 2>/dev/null || true

# Republish vendor assets (force overwrite)
info "  Republicando assets de vendor..."
sudo -u "${APP_USER}" php artisan vendor:publish --all --force 2>/dev/null || true

# Storage link
sudo -u "${APP_USER}" php artisan storage:link 2>/dev/null || true

# Rebuild caches
sudo -u "${APP_USER}" php artisan config:cache 2>/dev/null || true
sudo -u "${APP_USER}" php artisan route:cache 2>/dev/null || true
sudo -u "${APP_USER}" php artisan view:cache 2>/dev/null || true

# Ensure installed marker
touch "${CRM_DIR}/storage/installed"
chown ${APP_USER}:${APP_USER} "${CRM_DIR}/storage/installed"

# Restore permissions for www-data (PHP-FPM)
chown -R ${APP_USER}:${APP_USER} "${CRM_DIR}"
chown -R www-data:www-data "${CRM_DIR}/storage" 2>/dev/null || true
chown -R www-data:www-data "${CRM_DIR}/bootstrap/cache" 2>/dev/null || true
chmod -R 775 "${CRM_DIR}/storage" 2>/dev/null || true
chmod -R 775 "${CRM_DIR}/bootstrap/cache" 2>/dev/null || true
chmod 755 /home/${APP_USER}
chmod 755 "${CRM_DIR}"

ok "Caches limpiados y reconstruidos"

# ── 8. Restart services ──────────────────────────────────────
info "[8/8] Reiniciando servicios..."

systemctl restart php*-fpm 2>/dev/null || true
systemctl restart nginx 2>/dev/null || true

ok "PHP-FPM y nginx reiniciados"

# ── Summary ──────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo -e "  ${GREEN}KANBAN FIX COMPLETO${NC}"
echo "══════════════════════════════════════════════════════"
echo ""
echo "  Cambios realizados:"
echo "    - Pipeline rotten_days = 30 (evita iconos rojos falsos)"
echo "    - Stages con sort_order correcto (1-6)"
echo "    - Todos los leads activos en stage 'Nuevo'"
echo "    - user_id asignado a todos los leads"
echo "    - Caches limpiados y reconstruidos"
echo "    - Assets republicados"
echo "    - Servicios reiniciados"
echo ""
echo "  Prueba ahora: http://$(curl -s -4 ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')/admin/leads"
echo ""
echo "══════════════════════════════════════════════════════"
echo ""
