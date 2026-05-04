# 🚀 Insulleads Setup Guide

## Quick Start (5 minutos)

### Opción 1: Setup Automático (RECOMENDADO)

```bash
# Clonar repo
git clone https://github.com/gb0x21/insulleads
cd insulleads

# Ejecutar setup automático (instala todo)
bash setup.sh

# Editar .env con tus API keys
nano .env  # o vim, o tu editor favorito
```

El script `setup.sh` hace automáticamente:
- ✅ Verifica Python 3.10+
- ✅ Crea virtual environment
- ✅ Instala `requirements.txt`
- ✅ Instala Playwright browsers
- ✅ Crea `.env` desde `.env.example`
- ✅ Inicializa SQLite databases
- ✅ Testa Telegram (si está configurado)

### Opción 2: Manual Step-by-Step

```bash
# 1. Clonar y entrar al directorio
git clone https://github.com/gb0x21/insulleads
cd insulleads

# 2. Crear virtual environment
python3 -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# 3. Instalar dependencias Python
pip install -r requirements.txt

# 4. Instalar Playwright browsers
playwright install chromium

# 5. Crear archivo .env
cp .env.example .env
nano .env  # Editar con tus API keys

# 6. Inicializar databases
python main.py  # Automáticamente llama init_db()

# 7. Test Telegram
python main.py --test
```

---

## ⚙️ Configuración Mínima (.env)

Para que funcione básicamente, necesitas **al menos estos 3 valores**:

```bash
# Token del bot Telegram (para notificaciones)
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklmnoPQRstuvwxyz
TELEGRAM_CHAT_ID=-1001234567890

# (Opcional pero recomendado) Firecrawl para web scraping
FIRECRAWL_API_KEY=fc-...
```

¿No tienes token de Telegram? Crea uno en 2 minutos:
```bash
# 1. Abre Telegram
# 2. Busca @BotFather
# 3. Escribe /newbot
# 4. Sigue las instrucciones
# 5. Copia el token a .env
```

¿No tienes Firecrawl API key?
```bash
# Opcional (sin ella, solo usa Socrata/CKAN/SeeClickFix)
# Para obtener: https://firecrawl.dev
```

---

## 📊 Mapa de Dependencias

```
┌─────────────────────────────────────────────────────┐
│           EXTERNAL REPOS / LIBRARIES                │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Python Core (3.10+)                                │
│  ├─ requirements.txt (7 packages):                  │
│  │  ├─ requests — HTTP calls to APIs                │
│  │  ├─ python-dotenv — .env config loader           │
│  │  ├─ schedule — Job scheduling (legacy)           │
│  │  ├─ beautifulsoup4 — HTML parsing                │
│  │  ├─ firecrawl-py — Web scraping SDK              │
│  │  ├─ playwright — Browser automation              │
│  │  └─ markitdown — Document to markdown            │
│  │                                                  │
│  External Services (APIs):                          │
│  ├─ Telegram Bot API — notifications                │
│  ├─ Socrata, CKAN — municipal data                  │
│  ├─ SeeClickFix — 311 reports                       │
│  ├─ NREL Solar — solar potential                    │
│  ├─ (Optional) Firecrawl — advanced scraping        │
│  └─ (Optional) Yelp, Google Places — biz data       │
│                                                     │
│  Local Services (Optional):                         │
│  └─ MySQL — Krayin CRM for lead management          │
│                                                     │
│  Claude Tools (Optional):                           │
│  └─ gstack — 45 Claude Code slash commands          │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## 🎯 Verificar Setup Completo

Después de ejecutar `setup.sh`, verifica que todo funciona:

```bash
# 1. Test Telegram
python main.py --test

# 2. Ejecutar un agente (ej: permits)
python main.py --run permits

# 3. Ver estadísticas
python main.py --stats

# 4. Ver health report
python main.py --health
```

**Esperado:**
- ✅ `--test`: Telegram message enviado sin errores
- ✅ `--run permits`: 10-100 leads encontrados
- ✅ `--stats`: Tabla con conteos por agente
- ✅ `--health`: Resumen de estado de agentes

Si algo falla, revisa la sección **Troubleshooting** más abajo.

---

## 📦 Estructura de Directorios (después de setup)

```
insulleads/
├── setup.sh                    ← Script de setup automático
├── SETUP_GUIDE.md             ← Este archivo
├── DEPENDENCIES.md            ← Repos y APIs necesarios
├── CLAUDE.md                  ← Guías de desarrollo
├── requirements.txt           ← Dependencias Python
├── .env.example              ← Plantilla de configuración
├── .env                      ← Tu configuración (generado por setup)
│
├── venv/                      ← Virtual environment (creado por setup)
│
├── main.py                    ← Scheduler principal
├── agents/                    ← 13 agentes
│   ├── base.py
│   ├── permits_agent.py
│   ├── solar_agent.py
│   └── ...
├── utils/                     ← Utilities compartidos
│   ├── db.py                 ← SQLite init
│   ├── telegram.py           ← Notifications
│   ├── dedup.py              ← Cross-agent dedup
│   ├── memory.py             ← Memory engine (FTS5)
│   ├── agent_metrics.py      ← Circuit breaker, adaptive intervals
│   ├── lead_scoring.py       ← Lead scoring with memory
│   ├── lead_outcomes.py      ← CRM feedback loop
│   ├── firecrawl_client.py   ← Web scraping API
│   ├── playwright_scraper.py ← Browser automation
│   ├── markitdown_client.py  ← Document conversion
│   └── ...
├── outreach/                  ← Django CRM integration
│   ├── models.py
│   ├── views.py
│   └── tests/
│
├── data/                      ← Generated by setup
│   ├── leads.db              ← SQLite (sent_leads, consolidated_leads, etc)
│   └── crm_config.json       ← CRM config (si usas Krayin)
│
└── .claude/                   ← Claude Code settings (opcional)
    └── skills/gstack/        ← gstack slash commands (opcional)
```

---

## 🔧 Troubleshooting

### Error: "Python 3.10+ required"
```bash
python3 --version
# Si es < 3.10, instala Python 3.11 o 3.12
```

### Error: "TELEGRAM_BOT_TOKEN not configured"
```bash
# Abre .env y edita:
TELEGRAM_BOT_TOKEN=tu_token_aqui
TELEGRAM_CHAT_ID=tu_chat_id_aqui
```

### Error: "Telegram connection failed"
```bash
# Verifica que el token sea correcto y que tu bot exista
python -c "
import requests
token = open('.env').read().split('TELEGRAM_BOT_TOKEN=')[1].split('\n')[0].strip()
resp = requests.get(f'https://api.telegram.org/bot{token}/getMe')
print(resp.json())
"
```

### Error: "playwright install failed"
```bash
# En algunos sistemas necesita deps del SO:
# Ubuntu/Debian:
sudo apt install libgconf-2-4 libxss1

# macOS:
brew install ffmpeg

# Luego reintentar:
playwright install chromium
```

### Error: "Database error" o "table consolidated_leads not found"
```bash
# Reset databases:
rm -f data/leads.db
python main.py  # Recreará todas las tablas
```

### Error: "cannot import from utils"
```bash
# Asegúrate que estés en la raíz del proyecto:
pwd  # Debería terminar en /insulleads

# Y que venv esté activado:
which python  # Debería decir venv/bin/python
```

---

## 📝 Próximos Pasos después de Setup

### 1. Ejecutar un agente (test)
```bash
python main.py --run permits
# Deberías ver 10-100 leads de permisos
```

### 2. Ver datos en terminal
```bash
python main.py --stats
# Ver conteos de leads por agente
```

### 3. (Opcional) Setup Krayin CRM
```bash
python utils/crm_setup.py
# Para almacenar leads en un CRM web
```

### 4. (Opcional) Setup gstack skills para Claude Code
```bash
git clone https://github.com/garrytan/gstack /tmp/gstack
# Luego copiar a ~/.claude/skills/gstack/
```

### 5. (Opcional) Agregar más APIs
Editar `.env` con más keys (Yelp, NREL, Firecrawl, etc)
para que más agentes funcionen a full capacity.

---

## 📚 Documentación Relevante

- **DEPENDENCIES.md** — Detalle de todos los repos y APIs
- **CLAUDE.md** — Guías de desarrollo
- **agents/\*_agent.py** — Código de cada agente
- **utils/memory.py** — Motor de memoria (contexto entre ciclos)
- **utils/agent_metrics.py** — Circuit breaker y adaptive scheduling

---

## 🆘 ¿Algo no funciona?

1. **Verifica .env**: `cat .env | grep -E "^(TELEGRAM|FIRECRAWL)"`
2. **Chequea la venv**: `which python` debe decir `venv/bin/python`
3. **Test de conectividad**: `python main.py --test`
4. **Revisa los logs**: `python main.py --run permits 2>&1 | tail -50`
5. **Lee DEPENDENCIES.md** para ver qué APIs requiere cada agente

¿Aún no funciona? Revisa `CLAUDE.md` sección "Comandos Útiles".

