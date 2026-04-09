#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
#  debug_kanban.sh — Diagnose and fix Kanban rendering issues
#
#  Checks: Vite build, CSS/JS assets, fonts, lead data format
#  Fixes:  Rebuilds assets if corrupted, creates test lead via Eloquent
#
#  Usage:  sudo bash utils/debug_kanban.sh
# ══════════════════════════════════════════════════════════════

set -uo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

CRM_DIR="/home/insulleads/krayin-crm"
APP_USER="insulleads"

info()  { echo -e "${CYAN}[INFO]${NC}  $1"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $1"; }

echo ""
echo "══════════════════════════════════════════════════════"
echo "  Kanban Debug & Rebuild"
echo "══════════════════════════════════════════════════════"
echo ""

# ── 1. Check Vite build directory ────────────────────────────
info "[1/9] Verificando directorio de build..."

if [ -d "${CRM_DIR}/public/build" ]; then
    ok "public/build/ existe"
    echo "  Contenido:"
    ls -la "${CRM_DIR}/public/build/" 2>/dev/null | head -10
    echo ""

    if [ -f "${CRM_DIR}/public/build/manifest.json" ]; then
        ok "manifest.json existe"
        echo "  Primeras 30 lineas:"
        head -30 "${CRM_DIR}/public/build/manifest.json"
        echo ""
    else
        fail "manifest.json NO existe — assets no compilados"
    fi
else
    fail "public/build/ NO existe — Vite build necesario"
fi

# ── 2. Check CSS files ──────────────────────────────────────
info "[2/9] Verificando archivos CSS..."

CSS_COUNT=$(find "${CRM_DIR}/public/build" -name "*.css" 2>/dev/null | wc -l)
if [ "${CSS_COUNT}" -gt 0 ]; then
    ok "${CSS_COUNT} archivo(s) CSS encontrados:"
    find "${CRM_DIR}/public/build" -name "*.css" -exec ls -lh {} \; 2>/dev/null
    echo ""

    # Check if Tailwind classes exist in the CSS
    CSS_FILE=$(find "${CRM_DIR}/public/build" -name "*.css" 2>/dev/null | head -1)
    if [ -n "${CSS_FILE}" ]; then
        # Check for key Tailwind classes used in Kanban cards
        for CLASS in "flex-col" "rounded-md" "bg-gray-50" "lead-item" "text-rose-600" "icon-rotten"; do
            if grep -q "${CLASS}" "${CSS_FILE}" 2>/dev/null; then
                ok "  CSS contiene '${CLASS}'"
            else
                fail "  CSS NO contiene '${CLASS}' — Tailwind incompleto"
            fi
        done
    fi
else
    fail "No hay archivos CSS en build/ — rebuild necesario"
fi
echo ""

# ── 3. Check JS files ───────────────────────────────────────
info "[3/9] Verificando archivos JavaScript..."

JS_COUNT=$(find "${CRM_DIR}/public/build" -name "*.js" 2>/dev/null | wc -l)
if [ "${JS_COUNT}" -gt 0 ]; then
    ok "${JS_COUNT} archivo(s) JS encontrados:"
    find "${CRM_DIR}/public/build" -name "*.js" -exec ls -lh {} \; 2>/dev/null
    echo ""

    # Check for Vue/Kanban-related code in JS
    JS_FILE=$(find "${CRM_DIR}/public/build" -name "*.js" -size +100k 2>/dev/null | head -1)
    if [ -n "${JS_FILE}" ]; then
        for KEYWORD in "draggable" "kanban" "stageLeads" "lead-item"; do
            if grep -q "${KEYWORD}" "${JS_FILE}" 2>/dev/null; then
                ok "  JS contiene '${KEYWORD}'"
            else
                warn "  JS NO contiene '${KEYWORD}'"
            fi
        done
    fi
else
    fail "No hay archivos JS en build/"
fi
echo ""

# ── 4. Check font files ─────────────────────────────────────
info "[4/9] Verificando fonts..."

FONT_COUNT=$(find "${CRM_DIR}/public" -name "*.woff" -o -name "*.woff2" -o -name "*.ttf" 2>/dev/null | wc -l)
if [ "${FONT_COUNT}" -gt 0 ]; then
    ok "${FONT_COUNT} font(s) encontrados:"
    find "${CRM_DIR}/public" \( -name "*.woff" -o -name "*.woff2" -o -name "*.ttf" \) -exec ls -lh {} \; 2>/dev/null
else
    fail "No hay fonts en public/"
fi
echo ""

# ── 5. Check HTML page source (CSS/JS includes) ─────────────
info "[5/9] Verificando HTML source de la pagina..."

# Get CSRF token and session cookie
COOKIE_JAR=$(mktemp)
LOGIN_PAGE=$(curl -s -c "${COOKIE_JAR}" http://localhost/admin/login 2>/dev/null)
CSRF_TOKEN=$(echo "${LOGIN_PAGE}" | grep -oP 'name="_token"[^>]*value="\K[^"]+' 2>/dev/null | head -1)

if [ -n "${CSRF_TOKEN}" ]; then
    # Login
    curl -s -b "${COOKIE_JAR}" -c "${COOKIE_JAR}" \
        -X POST http://localhost/admin/login \
        -d "_token=${CSRF_TOKEN}&email=admin%40example.com&password=admin123" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -L -o /dev/null 2>/dev/null

    # Fetch leads page
    LEADS_HTML=$(curl -s -b "${COOKIE_JAR}" http://localhost/admin/leads 2>/dev/null)

    echo "  CSS/JS includes en la pagina:"
    echo "${LEADS_HTML}" | grep -oP '(href|src)="[^"]*\.(css|js)[^"]*"' 2>/dev/null | head -20
    echo ""

    # Check for Vite references
    if echo "${LEADS_HTML}" | grep -q "build/assets" 2>/dev/null; then
        ok "Pagina referencia assets de Vite build"
    else
        fail "Pagina NO referencia assets de Vite build"
    fi

    # Check for inline errors
    if echo "${LEADS_HTML}" | grep -q "Error\|Exception\|Whoops" 2>/dev/null; then
        fail "Pagina contiene errores:"
        echo "${LEADS_HTML}" | grep -i "error\|exception\|whoops" | head -5
    else
        ok "No hay errores visibles en HTML"
    fi

    # Save full HTML for manual inspection
    echo "${LEADS_HTML}" > /tmp/kanban_page.html
    ok "HTML completo guardado en /tmp/kanban_page.html"
    echo "  Tamano: $(wc -c < /tmp/kanban_page.html) bytes"
    echo ""

    # ── 6. Fetch Kanban API data ─────────────────────────────
    info "[6/9] Verificando respuesta API del Kanban..."

    API_RESPONSE=$(curl -s -b "${COOKIE_JAR}" \
        "http://localhost/admin/leads/get?pipeline_id=1&pipeline_stage_id=1&page=1" \
        -H "X-Requested-With: XMLHttpRequest" \
        -H "Accept: application/json" 2>/dev/null)

    if echo "${API_RESPONSE}" | python3 -m json.tool > /dev/null 2>&1; then
        ok "API retorna JSON valido"
        API_LEN=$(echo "${API_RESPONSE}" | wc -c)
        echo "  Tamano respuesta: ${API_LEN} bytes"

        # Show first lead structure
        echo "  Primer lead (estructura):"
        echo "${API_RESPONSE}" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    # Find first stage with leads
    for key, stage in data.items():
        leads = stage.get('leads', {}).get('data', [])
        if leads:
            lead = leads[0]
            # Show structure with truncated values
            for k, v in lead.items():
                val = str(v)
                if len(val) > 80:
                    val = val[:80] + '...'
                print(f'    {k}: {val}')
            break
    else:
        print('    No leads found in response')
except Exception as e:
    print(f'    Error parsing: {e}')
" 2>/dev/null
    else
        fail "API NO retorna JSON valido"
        echo "  Primeros 500 chars:"
        echo "${API_RESPONSE}" | head -c 500
    fi
    echo ""
else
    warn "No se pudo obtener CSRF token — verificacion de pagina omitida"
fi

rm -f "${COOKIE_JAR}" 2>/dev/null

# ── 7. Check lead data in MySQL ──────────────────────────────
info "[7/9] Verificando datos de leads en MySQL..."

echo "  Ejemplo de lead (primer activo):"
mysql -u root -e "
    SELECT l.id, LEFT(l.title, 40) as title,
           LENGTH(l.description) as desc_len,
           l.lead_value, l.person_id, l.lead_source_id,
           l.lead_type_id, l.user_id,
           l.lead_pipeline_id, l.lead_pipeline_stage_id,
           l.expected_close_date, l.created_at
    FROM krayin_crm.leads l
    WHERE l.status = 1
    ORDER BY l.id
    LIMIT 3;
" 2>/dev/null
echo ""

# Check for problematic characters in lead data
info "  Buscando caracteres problematicos en titles..."
PROBLEM_TITLES=$(mysql -u root -sN -e "
    SELECT COUNT(*) FROM krayin_crm.leads
    WHERE status = 1 AND (
        title LIKE '%<%' OR title LIKE '%>%' OR
        title LIKE '%\\\"%' OR title REGEXP '[^[:print:]]'
    );
" 2>/dev/null)
if [ "${PROBLEM_TITLES}" != "0" ] && [ -n "${PROBLEM_TITLES}" ]; then
    warn "${PROBLEM_TITLES} leads con caracteres problematicos en title"
    mysql -u root -e "
        SELECT id, title FROM krayin_crm.leads
        WHERE status = 1 AND (
            title LIKE '%<%' OR title LIKE '%>%' OR
            title LIKE '%\\\"%' OR title REGEXP '[^[:print:]]'
        ) LIMIT 5;
    " 2>/dev/null
else
    ok "No hay caracteres problematicos en titles"
fi
echo ""

# Check for NULL relationships
info "  Verificando relaciones de leads..."
mysql -u root -e "
    SELECT
        COUNT(*) as total,
        SUM(person_id IS NULL) as sin_persona,
        SUM(lead_source_id IS NULL) as sin_source,
        SUM(lead_type_id IS NULL) as sin_type,
        SUM(user_id IS NULL) as sin_user,
        SUM(lead_pipeline_stage_id IS NULL) as sin_stage,
        SUM(expected_close_date IS NULL) as sin_close_date
    FROM krayin_crm.leads
    WHERE status = 1;
" 2>/dev/null
echo ""

# ── 8. Create test lead through Eloquent ─────────────────────
info "[8/9] Creando lead de prueba via Eloquent (metodo correcto de Krayin)..."

cd "${CRM_DIR}"
sudo -u "${APP_USER}" php artisan tinker --execute="
    \$pipeline = \Webkul\Lead\Models\Pipeline::first();
    \$stage = \$pipeline->stages()->orderBy('sort_order')->first();
    \$user = \Webkul\User\Models\User::first();

    // Create person first
    \$person = \Webkul\Contact\Models\Person::create([
        'name' => 'Test Person Kanban',
    ]);

    // Create lead through model
    \$lead = \Webkul\Lead\Models\Lead::create([
        'title' => 'TEST-KANBAN Lead de Prueba',
        'description' => 'Este es un lead de prueba creado via Eloquent para diagnosticar el Kanban.',
        'lead_value' => 5000,
        'status' => 1,
        'lead_pipeline_id' => \$pipeline->id,
        'lead_pipeline_stage_id' => \$stage->id,
        'user_id' => \$user->id,
        'person_id' => \$person->id,
        'expected_close_date' => now()->addDays(30),
    ]);

    echo 'Lead de prueba creado: ID=' . \$lead->id . ', title=' . \$lead->title . PHP_EOL;
    echo 'Person: ' . \$person->name . ' (ID=' . \$person->id . ')' . PHP_EOL;
    echo 'Pipeline: ' . \$pipeline->name . ', Stage: ' . \$stage->name . PHP_EOL;
" 2>/dev/null && ok "Lead de prueba creado via Eloquent" || warn "No se pudo crear lead de prueba"

echo ""

# ── 9. Try to rebuild Vite assets if needed ──────────────────
info "[9/9] Verificando si se necesita rebuild de assets..."

if [ -f "${CRM_DIR}/package.json" ]; then
    ok "package.json existe"

    if [ -d "${CRM_DIR}/node_modules" ]; then
        ok "node_modules existe"
    else
        warn "node_modules no existe — instalando..."
        cd "${CRM_DIR}"
        sudo -u "${APP_USER}" npm install 2>/dev/null && ok "npm install completado" || fail "npm install fallo"
    fi

    # Check if rebuild is needed
    CSS_FILE=$(find "${CRM_DIR}/public/build" -name "*.css" 2>/dev/null | head -1)
    NEEDS_REBUILD=false

    if [ -z "${CSS_FILE}" ]; then
        NEEDS_REBUILD=true
        fail "No hay CSS compilado"
    elif ! grep -q "lead-item\|flex-col\|rounded-md" "${CSS_FILE}" 2>/dev/null; then
        NEEDS_REBUILD=true
        warn "CSS no contiene clases criticas del Kanban"
    fi

    if [ "${NEEDS_REBUILD}" = true ]; then
        info "Reconstruyendo assets con Vite..."
        cd "${CRM_DIR}"
        sudo -u "${APP_USER}" npm run build 2>&1 | tail -20
        echo ""

        if [ $? -eq 0 ]; then
            ok "Assets reconstruidos"
        else
            fail "Rebuild fallo"
        fi

        # Fix permissions after build
        chown -R ${APP_USER}:${APP_USER} "${CRM_DIR}/public/build" 2>/dev/null || true
        chmod -R 755 "${CRM_DIR}/public/build" 2>/dev/null || true
    else
        ok "CSS parece completo — rebuild no necesario"
    fi
else
    warn "No hay package.json — assets precompilados del composer"
fi

echo ""

# ── Summary ──────────────────────────────────────────────────
echo "══════════════════════════════════════════════════════"
echo "  Diagnostico completo"
echo "══════════════════════════════════════════════════════"
echo ""
echo "  Archivos de diagnostico:"
echo "    HTML pagina:  /tmp/kanban_page.html"
echo ""
echo "  Proximos pasos:"
echo "    1. Revisa http://165.232.151.158/admin/leads"
echo "    2. Busca el lead 'TEST-KANBAN Lead de Prueba'"
echo "    3. Si el test lead se ve bien pero los demas no → problema de datos"
echo "    4. Si el test lead tambien se ve mal → problema de CSS/JS"
echo ""
echo "══════════════════════════════════════════════════════"
echo ""
