# 🌐 Insulleads — Repos y Librerías Summary

## 📊 Quick Reference

### **External Repos INTEGRADOS** (en uso activo)

```
┌────────────────────────────────────────────────────────────────────┐
│ REPO                    │ PROPÓSITO           │ INSTALACIÓN        │
├────────────────────────────────────────────────────────────────────┤
│ firecrawl/firecrawl    │ Web scraping API    │ pip install ...py  │
│ microsoft/playwright   │ Browser automation  │ pip install ... &  │
│                        │                     │ playwright install │
│ microsoft/markitdown   │ Doc → Markdown      │ pip install ...    │
│ garrytan/gstack        │ Claude Code skills  │ git clone & copy   │
└────────────────────────────────────────────────────────────────────┘
```

### **Conceptos Inspiradores** (implementados, no repos)

```
┌────────────────────────────────────────────────────────────────────┐
│ CONCEPT                │ UBICACIÓN           │ QUÉ APORTA         │
├────────────────────────────────────────────────────────────────────┤
│ claude-mem             │ utils/memory.py     │ SQLite FTS5 search │
│ hermes-agent           │ utils/agent_..py    │ Circuit breaker +  │
│                        │                     │ Adaptive intervals │
│ karpathy-principles    │ CLAUDE.md           │ Dev guidelines     │
└────────────────────────────────────────────────────────────────────┘
```

---

## 📦 Instalación por Categoría

### **1. Python Core Packages** (en requirements.txt)

| Package | Versión | Para qué |
|---------|---------|----------|
| requests | >=2.31.0 | HTTP calls a APIs |
| python-dotenv | >=1.0.0 | Load .env vars |
| schedule | >=1.2.0 | Job scheduling (legacy) |
| beautifulsoup4 | >=4.12.0 | HTML parsing |
| **firecrawl-py** | >=1.0.0 | Firecrawl SDK |
| **playwright** | >=1.45.0 | Browser automation |
| **markitdown** | >=0.2.0 | Document conversion |

**Instalar todo:**
```bash
pip install -r requirements.txt
```

### **2. Browser Binaries** (Playwright)

```bash
playwright install chromium  # Después de: pip install playwright
```

### **3. APIs Gratuitas** (no requieren instalación, solo API keys)

| API | Agente | Costo | Setup |
|-----|--------|-------|-------|
| Socrata | permits, solar, energy | GRATIS | Incluido en portales |
| CKAN | permits, energy | GRATIS | Incluido en portales |
| SeeClickFix | rodents, flood | GRATIS | https://seeclickfix.com/api |
| NOAA | flood | GRATIS | https://api.weather.gov |
| Telegram Bot | all | GRATIS | @BotFather en Telegram |

### **4. APIs con Créditos Gratis** (requieren signup)

| API | Crédito/mes | Agente | Signup |
|-----|-------------|--------|--------|
| NREL Solar | N/A | solar | https://developer.nrel.gov |
| Google Places | $200 | places | https://console.cloud.google.com |
| Yelp Fusion | 5000 calls | yelp | https://www.yelp.com/developers |
| US Census | GRATIS | realestate | https://api.census.gov/data/key_signup.html |

### **5. APIs de Pago** (OPCIONAL)

| API | Costo | Agente | Para qué |
|-----|-------|--------|----------|
| Firecrawl | $99-999/mes | permits, const | Scraping avanzado |
| Shovels.ai | $199+/mes | permits | Agregador nacional |
| Hunter.io | $99+/mes | enrichment | Email finder |
| Twilio WhatsApp | $50+/mes | notifications | Canal WhatsApp |
| SendGrid | $15+/mes | notifications | Canal Email |

### **6. Servicios Locales** (OPCIONAL)

| Servicio | Puerto | Para qué | Docker |
|----------|--------|----------|--------|
| MySQL | 3306 | Krayin CRM | docker-compose up |
| Telegram | API | Notifications | N/A |

### **7. Claude Code Tools** (OPCIONAL)

| Tool | Tipo | Instalación |
|------|------|-------------|
| gstack | 45 skills | git clone + copy to ~/.claude/skills/ |

---

## 🔗 Dependency Graph

```
Insulleads Core
  ├─ Python 3.10+
  ├─ requirements.txt
  │  ├─ requests → APIs (Socrata, CKAN, SeeClickFix, NREL, etc)
  │  ├─ python-dotenv → Load .env config
  │  ├─ beautifulsoup4 → Parse HTML
  │  ├─ firecrawl-py → Web scraping
  │  ├─ playwright → Browser automation
  │  │  └─ chromium binary (installed via: playwright install)
  │  └─ markitdown → Document conversion
  │
  ├─ SQLite (bundled with Python)
  │  ├─ sent_leads table
  │  ├─ consolidated_leads table (dedup)
  │  ├─ memories table (FTS5)
  │  ├─ lead_outcomes table
  │  └─ agent_runs, agent_state tables (metrics)
  │
  ├─ Telegram Bot API (para notificaciones)
  │  └─ TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID en .env
  │
  └─ (Optional) MySQL + Krayin CRM
     └─ utils/crm_sync.py → sync leads a CRM
```

---

## 📋 Setup Checklist

- [ ] **Python 3.10+** instalado
- [ ] **pip install -r requirements.txt** ejecutado
- [ ] **playwright install chromium** ejecutado
- [ ] **.env** creado con TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID
- [ ] **python main.py --test** funciona (Telegram test)
- [ ] **python main.py --run permits** obtiene leads
- [ ] **(Optional) python utils/crm_setup.py** si usas Krayin

---

## 🎯 Recommended Setup Path

### **Minimal** (para probar)
1. Clone repo
2. Run `bash setup.sh`
3. Edit `.env` con TELEGRAM keys
4. Test: `python main.py --test`

### **Production** (para usar en serio)
1. Todo lo de Minimal +
2. Agregar más API keys (.env)
3. Setup Krayin: `python utils/crm_setup.py`
4. Cron job: ejecutar `python main.py` cada minuto
5. Monitoring: `python main.py --health` cada hora

### **Full Stack** (con todas las herramientas)
1. Todo lo de Production +
2. Install gstack skills para Claude Code
3. Setup claude-mem para tu sesión (opcional)
4. Configurar múltiples agentes con diferentes APIs

---

## 📚 Documentación

| Archivo | Para qué |
|---------|----------|
| **SETUP_GUIDE.md** | Guía paso-a-paso de instalación |
| **DEPENDENCIES.md** | Detalle completo de repos y APIs |
| **CLAUDE.md** | Guías de desarrollo y reglas |
| **setup.sh** | Script automático de setup |
| **.env.example** | Plantilla de configuración |

---

## ✅ Verificación Post-Setup

```bash
# 1. Chequear venv
which python  # Debe decir: /path/to/venv/bin/python

# 2. Chequear dependencias
python -c "import firecrawl; import playwright; import markitdown; print('OK')"

# 3. Chequear SQLite
python -c "from utils.db import init_db; init_db(); print('DB OK')"

# 4. Chequear Telegram
python main.py --test

# 5. Chequear un agente
python main.py --run permits
```

---

## 🔗 Recursos Externos

### Official Repos
- https://github.com/firecrawl/firecrawl
- https://github.com/microsoft/playwright
- https://github.com/microsoft/markitdown
- https://github.com/garrytan/gstack

### APIs Públicas
- https://dev.socrata.com (municipal data)
- https://docs.ckan.org (open data)
- https://seeclickfix.com/api (311 reports)
- https://api.weather.gov (NOAA weather)

### Conceptos
- https://github.com/thedotmack/claude-mem (memory persistence)
- https://github.com/NousResearch/hermes-agent (agent patterns)
- https://github.com/forrestchang/andrej-karpathy-skills (dev principles)

