# 📦 Insulleads — Dependencias y Repos Externos

## 🔗 Repos Externos que Mejoran el Proyecto

### **INTEGRADOS (en uso)**

| Repo | Propósito | Qué Aporta | Instalación |
|------|-----------|-----------|------------|
| **firecrawl/firecrawl** | Web scraping sin API | `utils/firecrawl_client.py` — convierte URLs a markdown/JSON | `pip install firecrawl-py` |
| **microsoft/playwright** | Browser automation | `utils/playwright_scraper.py` — portales JS-rendered, paginación, login | `pip install playwright && playwright install chromium` |
| **microsoft/markitdown** | Doc → Markdown | `utils/markitdown_client.py` — PDFs, Word, Excel, imágenes a markdown | `pip install markitdown` |
| **garrytan/gstack** | Claude Code skills | 45 slash commands (`/review`, `/qa`, `/cso`, `/ship`, etc.) | `git clone; install en ~/.claude/skills/gstack/` |

### **INSPIRADORES (patrones implementados, no repo)**

| Concepto | Donde Aparece | Qué Implementamos |
|----------|---------------|-------------------|
| **claude-mem** | `utils/memory.py` | SQLite FTS5 + hash embeddings para agentes |
| **hermes-agent** | `utils/agent_metrics.py` | Circuit breaker + adaptive intervals |
| **karpathy principles** | `CLAUDE.md` | Think Before → Simplicity → Surgical Changes → Goal-Driven |

---

## 📋 Dependencias Python (requirements.txt)

```
requests>=2.31.0           # HTTP requests (APIs)
python-dotenv>=1.0.0       # Load .env variables
schedule>=1.2.0            # Job scheduling (legacy, reemplazado por main.py)
beautifulsoup4>=4.12.0     # HTML parsing
firecrawl-py>=1.0.0        # Firecrawl API SDK
playwright>=1.45.0         # Browser automation
markitdown>=0.2.0          # Document conversion
```

---

## 🌐 Dependencias Externas (APIs, Servicios)

### **Datos Públicos (GRATIS)**

| API | Agente | Qué obtiene | Signup |
|-----|--------|------------|--------|
| **Socrata** | permits, solar, energy | Portales municipales (Oakland, SF, etc.) | Incluido en portales |
| **CKAN** | permits, energy | Datos abiertos (San José, etc.) | Incluido en portales |
| **SeeClickFix** | rodents, flood, places | Reportes ciudadanos 311 | https://seeclickfix.com/api |
| **NOAA** | flood | Flood warnings por condado | https://api.weather.gov |
| **USGS** | flood | Zonas propensas a inundaciones | https://earthquake.usgs.gov |

### **APIs con Créditos Gratis**

| API | Agente | Costo | Signup |
|-----|--------|-------|--------|
| **NREL Solar** | solar | $0.40/request | https://developer.nrel.gov |
| **Google Places** | places | $200/mes crédito gratis | https://console.cloud.google.com |
| **Yelp Fusion** | yelp | 5,000 calls/día | https://www.yelp.com/developers |
| **Census Bureau** | realestate | Gratis | https://api.census.gov/data/key_signup.html |

### **APIs de Pago (Opcional)**

| API | Agente | Costo/mes | Para qué |
|-----|--------|-----------|----------|
| **Firecrawl** | permits, construction | $99-999 | Scraping avanzado |
| **Shovels.ai** | permits | $199+ | Agregador nacional de permisos |
| **Hunter.io** | enrichment | $99+ | Email finder para contractors |
| **Twilio WhatsApp** | notifications | $50+ | Canal WhatsApp |
| **SendGrid** | notifications | $15+ | Canal Email |

### **Servicios Locales (REQUIEREN SETUP)**

| Servicio | Puerto | Para qué | Setup |
|----------|--------|----------|-------|
| **MySQL** (Krayin CRM) | 3306 | Almacenar leads en CRM | `docker-compose up krayin` |
| **Telegram Bot** | N/A | Notificaciones | Crear bot en @BotFather |

---

## 📁 Archivos de Setup Existentes

### **1. requirements.txt**
```bash
pip install -r requirements.txt  # Instala todas las deps Python
```

### **2. .env.example**
```bash
cp .env.example .env             # Crear config base
# Editar .env con tus API keys
```

### **3. main.py (CLI)**
```bash
python main.py --run permits     # Ejecutar agente puntual
python main.py --health          # Ver estado de agentes
python main.py --stats           # Ver stats de leads
python main.py --test            # Test Telegram
```

### **4. utils/db.py (init_db)**
```python
from utils.db import init_db
init_db()  # Crea todas las tablas SQLite
```

### **5. utils/crm_setup.py**
```bash
python utils/crm_setup.py  # Setup inicial de Krayin
```

### **⚠️ PROBLEMA ACTUAL:**
**No hay un script unificado que instale TODO automáticamente.**

---

## 🚀 Setup Completo (falta crear)

Actualmente necesitas:
```bash
# 1. Python venv
python -m venv venv
source venv/bin/activate

# 2. Deps Python
pip install -r requirements.txt

# 3. Playwright browsers
playwright install chromium

# 4. Copiar .env
cp .env.example .env

# 5. Editar .env con tus keys

# 6. Inicializar DBs
python main.py  # Llama init_db() automáticamente

# 7. Setup Krayin (si quieres usar CRM)
python utils/crm_setup.py

# 8. (Opcional) Instalar gstack skills
git clone https://github.com/garrytan/gstack /tmp/gstack
# ... copiar skills manualmente a ~/.claude/skills/gstack/
```

**Esto es tedioso. Deberías tener un setup.sh que lo haga todo.**

---

## 📊 Resumen: Qué Necesitas Instalar

```
┌─────────────────────────────────────────────┐
│         INSULLEADS SETUP COMPLETO           │
├─────────────────────────────────────────────┤
│                                             │
│ 1. Python 3.10+                             │
│ 2. pip install -r requirements.txt          │
│    └─ requests, python-dotenv, schedule,    │
│       beautifulsoup4, firecrawl-py,         │
│       playwright, markitdown                │
│                                             │
│ 3. playwright install chromium              │
│                                             │
│ 4. cp .env.example .env                     │
│    └─ Agregar APIs keys (Firecrawl,         │
│       Telegram, optional: Yelp, NREL, etc) │
│                                             │
│ 5. python main.py (inicializa DBs)          │
│                                             │
│ 6. (Opcional) Krayin CRM setup              │
│    └─ python utils/crm_setup.py             │
│                                             │
│ 7. (Opcional) gstack skills                 │
│    └─ Manual: git clone + copy to ~/.claude │
│                                             │
└─────────────────────────────────────────────┘
```

---

## ✅ Checklist de Setup

- [ ] Python 3.10+ instalado
- [ ] Virtual environment creado y activado
- [ ] `pip install -r requirements.txt` ejecutado
- [ ] `playwright install chromium` ejecutado
- [ ] `.env` creado y configurado (al menos TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID)
- [ ] `python main.py --test` funciona (Telegram test)
- [ ] `python main.py --run permits` obtiene leads sin error
- [ ] Base de datos inicializada (`data/leads.db` existe)
- [ ] (Opcional) Krayin CRM setup completado
- [ ] (Opcional) Firecrawl API key configurado para web scraping avanzado

