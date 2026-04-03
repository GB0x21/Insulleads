# Lead Generation Agents

Sistema automatizado de generacion de leads para contratistas de insulacion en el Bay Area.

Monitorea **54 ciudades** en **9 condados** del Bay Area usando APIs publicas y de pago, detecta oportunidades de insulacion, y envia alertas en tiempo real a Telegram con datos de contacto del GC.

---

## Agentes

| Agente | Fuentes | Ciudades | Intervalo | Oportunidad |
|--------|---------|----------|-----------|-------------|
| Permisos de Construccion | Socrata, CKAN | 54 (26 fuentes) | 60 min | ADU/remodel/addition = necesitan insulacion |
| Instalaciones Solares | Socrata, CKAN, NREL, Google Solar, Aurora, EnergySage | 54 (15 fuentes) | 60 min | Solar nuevo = mejorar aislamiento |
| Reportes 311 Plagas | SeeClickFix, Socrata, CKAN, Thumbtack | 54 (55 fuentes) | 2 hrs | Roedores/plagas = insulacion danada |
| Alertas NOAA Inundacion | NOAA Weather API | 13 zonas | 30 min | Agua = crawlspace danado |
| Construcciones Activas | Socrata, CKAN, BuildZoom | 54 (14 fuentes) | 60 min | Fase framing = insulacion es siguiente paso |
| Deconstruccion | Socrata, CKAN, ATTOM | 54 (14 fuentes) | 2 hrs | Demolicion/asbesto = insulacion nueva |
| Propiedades Vendidas | Socrata (assessor data) | 10 condados | 2 hrs | Nuevo dueno = renovacion probable |
| Eficiencia Energetica | Socrata (benchmarking) | SF, Oakland, SJ + condados | 6 hrs | Baja eficiencia = oportunidad insulacion |
| Google Places | Google Places API | Bay Area | 24 hrs | Negocios de construccion activos |
| Yelp Contractors | Yelp Fusion API | Bay Area | 24 hrs | Contratistas activos en la zona |

---

## Cobertura Geografica (54 Ciudades)

**Contra Costa County (19):** Pleasant Hill, Walnut Creek, Martinez, Clayton, Pittsburg, Lafayette, Orinda, Antioch, Moraga, Alamo, Danville, Hercules, Pinole, Oakley, San Ramon, Richmond, Brentwood, El Cerrito, Concord

**Alameda County (15):** Oakland, Berkeley, Fremont, Hayward, Dublin, Alameda, San Leandro, Pleasanton, Livermore, Newark, Castro Valley, San Lorenzo, Emeryville, Albany, Union City

**San Mateo County (7):** Daly City, South San Francisco, San Bruno, Millbrae, Burlingame, San Mateo, Redwood City

**Solano County (6):** Benicia, Fairfield, Vallejo, Suisun City, Rio Vista, Vacaville

**Santa Clara County (5):** San Jose, Sunnyvale, Santa Clara, Palo Alto, Mountain View

**Marin County (2):** Novato, San Rafael

**Sonoma County (2):** Sonoma, Petaluma

**Napa County (1):** Napa

**San Joaquin County (2):** Tracy, Stockton

**San Francisco County (1):** San Francisco

---

## Funcionalidades Avanzadas

### Cross-Agent Deduplication
Cuando la misma propiedad aparece en multiples agentes (ej: permiso de construccion + reporte de roedores + panel solar), se consolida en un "super lead" con datos fusionados y score boosteado.

### Hot Zone Detection
Clustering geografico en tiempo real. Cuando 3+ leads caen dentro de un radio de 500m, genera una alerta de "zona caliente" con link a Google Maps y recomendacion de campana puerta-a-puerta.

### Lead Scoring (0-100)
Score automatico basado en: valor del proyecto, tipo de proyecto, calidad de contacto, recencia, geografia, fuente, y senales de insulacion.

| Score | Grado | Accion |
|-------|-------|--------|
| 90-100 | HOT | Contactar de inmediato (WhatsApp + Email) |
| 70-89 | WARM | Alta prioridad (Email) |
| 50-69 | MEDIUM | Seguimiento estandar |
| 25-49 | COOL | Baja prioridad |
| 0-24 | COLD | Archivo |

### Multi-Channel Notifications
- **Telegram** — todos los leads en tiempo real
- **WhatsApp** (Twilio) — leads HOT al celular
- **Email** (SendGrid) — leads WARM + digest diario
- **Slack** — webhook para equipo

### Contact Enrichment
1. **CSVs locales** en `contacts/` — fuzzy matching por nombre
2. **CSLB** — California Contractors State License Board (web scrape)
3. **Hunter.io** — email finder por dominio
4. **Apollo.io** — enrichment de contactos

---

## Instalacion desde Cero en DigitalOcean Droplet

### Paso 1: Crear el Droplet

En DigitalOcean, crea un droplet con:
- **Image:** Ubuntu 24.04 LTS
- **Plan:** Basic $6/mes (1 vCPU, 1GB RAM) — suficiente para el sistema
- **Region:** San Francisco (SFO3) — mas cerca de las APIs
- **Authentication:** SSH Key (recomendado) o password

### Paso 2: Conectar al Droplet

```bash
ssh root@TU_IP_DEL_DROPLET
```

### Paso 3: Setup del Sistema

```bash
# Actualizar el sistema
apt update && apt upgrade -y

# Instalar Python 3.11+ y git
apt install -y python3 python3-pip python3-venv git

# Crear usuario para la app (no correr como root)
adduser --disabled-password --gecos "" insulleads
usermod -aG sudo insulleads
su - insulleads
```

### Paso 4: Clonar el Repositorio

```bash
cd ~
git clone https://github.com/GB0x21/Insulleads.git
cd Insulleads
```

### Paso 5: Crear Entorno Virtual

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Paso 6: Configurar Variables de Entorno

```bash
cp .env.example .env
nano .env
```

**Minimo requerido** (el sistema funciona solo con Telegram):
```
TELEGRAM_BOT_TOKEN=123456789:ABCdef...
TELEGRAM_CHAT_ID=-1001234567890
```

Para obtener estos valores:
1. En Telegram, busca **@BotFather** → `/newbot` → copia el token
2. Crea un grupo, agrega tu bot, envia un mensaje
3. Visita `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. Busca `"chat":{"id":` — ese numero negativo es tu Chat ID

**APIs opcionales** (agregar segun presupuesto):
```
# Gratuitas (recomendadas)
SOCRATA_APP_TOKEN=           # Evita throttling — gratis en data.sfgov.org
NREL_API_KEY=                # Potencial solar — gratis en developer.nrel.gov/signup
CENSUS_API_KEY=              # Demografia — gratis en api.census.gov/data/key_signup.html

# Pago Tier 1 (~$300/mes)
ATTOM_API_KEY=               # Datos de propiedad — $200/mes
HUNTER_API_KEY=              # Email finder — $49/mes (100 gratis)
SENDGRID_API_KEY=            # Email outreach — $15/mes
SENDGRID_FROM_EMAIL=leads@example.com
SENDGRID_TO_EMAIL=tu@email.com
GOOGLE_GEOCODE_API_KEY=      # Geocoding — ~$30/mes

# Pago Tier 2 (~$200/mes adicional)
GOOGLE_SOLAR_API_KEY=        # Solar por edificio — ~$50/mes
TWILIO_ACCOUNT_SID=          # WhatsApp alerts — $50/mes
TWILIO_AUTH_TOKEN=
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
TWILIO_WHATSAPP_TO=whatsapp:+1TUNUMERO
APOLLO_API_KEY=              # Contact enrichment — gratis/49/mes
THUMBTACK_API_KEY=           # Pest control leads — partner

# Pago Tier 3 (~$300-500/mes adicional)
BUILDZOOM_API_KEY=           # Tracking construccion — $100-300/mes
AURORA_API_KEY=              # Proyectos solar — $100+/mes
ENERGYSAGE_API_KEY=          # Compradores solar — partner
GOOGLE_PLACES_API_KEY=       # Negocios cercanos — $200 credito gratis/mes
YELP_API_KEY=                # Contratistas — 5000 calls/dia gratis

# Slack (opcional)
SLACK_WEBHOOK_URL=
```

### Paso 7: Agregar Contactos de GC

Copia tus archivos CSV de contratistas a la carpeta `contacts/`:
```bash
# Desde tu maquina local:
scp ~/Downloads/*.csv root@TU_IP:/home/insulleads/Insulleads/contacts/
```

Formatos soportados: cualquier CSV con columnas de nombre, telefono, email. El sistema detecta automaticamente las columnas.

### Paso 8: Probar Conexion

```bash
source venv/bin/activate
python main.py --test
```

Deberias ver un mensaje en tu grupo de Telegram. Si falla, revisa el token y chat ID en `.env`.

### Paso 9: Ejecutar Manualmente (prueba)

```bash
# Probar un agente individual
python main.py --run permits
python main.py --run solar
python main.py --run rodents

# Ver estadisticas
python main.py --stats
```

### Paso 10: Configurar como Servicio (24/7)

#### Opcion A: systemd (recomendado)

```bash
# Como root:
sudo nano /etc/systemd/system/insulleads.service
```

Pega este contenido:
```ini
[Unit]
Description=Lead Generation Agents
After=network.target

[Service]
Type=simple
User=insulleads
Group=insulleads
WorkingDirectory=/home/insulleads/Insulleads
ExecStart=/home/insulleads/Insulleads/venv/bin/python main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

# Variables de entorno
EnvironmentFile=/home/insulleads/Insulleads/.env

[Install]
WantedBy=multi-user.target
```

Activar:
```bash
sudo systemctl daemon-reload
sudo systemctl enable insulleads
sudo systemctl start insulleads

# Verificar que esta corriendo
sudo systemctl status insulleads

# Ver logs en tiempo real
sudo journalctl -u insulleads -f
```

#### Opcion B: Docker

```bash
docker build -t insulleads .
docker run -d \
  --name insulleads \
  --restart always \
  -v /home/insulleads/Insulleads/.env:/app/.env \
  -v /home/insulleads/Insulleads/data:/app/data \
  -v /home/insulleads/Insulleads/contacts:/app/contacts \
  insulleads

# Ver logs
docker logs -f insulleads
```

#### Opcion C: PM2

```bash
sudo apt install -y nodejs npm
sudo npm install -g pm2

cd /home/insulleads/Insulleads
pm2 start "venv/bin/python main.py" --name insulleads
pm2 save
pm2 startup
```

---

## Comandos

```bash
python main.py                    # Inicia todos los agentes
python main.py --test             # Prueba conexion Telegram
python main.py --run permits      # Ejecuta solo permisos
python main.py --run solar        # Ejecuta solo solar
python main.py --run rodents      # Ejecuta solo roedores
python main.py --run flood        # Ejecuta solo inundaciones
python main.py --run construction # Ejecuta solo construccion activa
python main.py --run deconstruction # Ejecuta solo deconstruccion
python main.py --run realestate   # Ejecuta solo propiedades vendidas
python main.py --run energy       # Ejecuta solo eficiencia energetica
python main.py --run places       # Ejecuta solo Google Places
python main.py --run yelp         # Ejecuta solo Yelp
python main.py --stats            # Estadisticas de leads enviados
```

### Habilitar/Deshabilitar Agentes

En `.env`, cambia a `false` para desactivar:
```
AGENT_PERMITS=true
AGENT_SOLAR=true
AGENT_RODENTS=true
AGENT_FLOOD=true
AGENT_CONSTRUCTION=true
AGENT_DECONSTRUCTION=true
AGENT_REALESTATE=true
AGENT_ENERGY=true
AGENT_PLACES=false     # Requiere Google Places API key
AGENT_YELP=false       # Requiere Yelp API key
```

---

## Estructura del Proyecto

```
Insulleads/
├── main.py                         # Orquestador — 10 agentes, paralelo
├── requirements.txt                # requests, python-dotenv, schedule, bs4
├── .env.example                    # Todas las variables documentadas
├── Dockerfile                      # Deploy con Docker
│
├── agents/
│   ├── base.py                     # BaseAgent v5 — dedup + hot zones
│   ├── permits_agent.py            # 26 fuentes — Socrata/CKAN
│   ├── solar_agent.py              # 15 fuentes + NREL/Google/Aurora/EnergySage
│   ├── rodents_agent.py            # 55 fuentes — SeeClickFix/Socrata/CKAN
│   ├── flood_agent.py              # 13 zonas NOAA
│   ├── construction_agent.py       # 14 fuentes — fases de construccion
│   ├── deconstruction_agent.py     # 14 fuentes — demolicion/asbesto
│   ├── realestate_agent.py         # 10 fuentes — ventas por condado
│   ├── energy_agent.py             # 8 fuentes — benchmarking/permits
│   ├── places_agent.py             # Google Places API
│   └── yelp_agent.py               # Yelp Fusion API
│
├── utils/
│   ├── db.py                       # SQLite — dedup de leads enviados
│   ├── telegram.py                 # Rate-limited Telegram sender
│   ├── contacts_loader.py          # 50K+ contactos CSV, fuzzy matching
│   ├── lead_scoring.py             # Score 0-100 con 7 factores
│   ├── dedup.py                    # Cross-agent deduplication engine
│   ├── hot_zones.py                # Geographic clustering (500m radius)
│   ├── census.py                   # US Census demographics
│   ├── contact_enrichment.py       # Hunter.io + Apollo.io
│   └── notifications.py            # SendGrid + WhatsApp + Slack
│
├── contacts/                       # CSVs de contratistas (tu data)
│   ├── B_CONTACTS_GC.csv
│   ├── C-2 INSULATION - CSLBSearchData.csv
│   ├── Real_State_The_Bay_Area.csv
│   └── ... (24 archivos CSV)
│
└── data/
    └── leads.db                    # Auto-creada — nunca repite leads
```

---

## Ejemplo de Mensaje en Telegram

```
🏗️ PERMISOS DE CONSTRUCCION — BAY AREA
━━━━━━━━━━━━━━━━━━━━
📌 Walnut Creek — 1845 Mt Diablo Blvd

▸ Ciudad: Walnut Creek
▸ Tipo de Permiso: REMODEL
▸ Descripcion: Kitchen and bathroom remodel with new insulation...
▸ Fecha Emision: 2026-03-28
▸ Valor Estimado: $85,000
▸ Contratista (GC): BAY AREA REMODELING INC
▸ Licencia CSLB: 987654
▸ Telefono GC: +19253820739  (via CSV B_CONTACTS_GC.csv)
▸ Email GC: info@bayarearemodeling.com
▸ Propietario: John Smith
▸ Lead Score: 🔥 92/100 (HOT) — Proyecto alto valor | Mencion directa de insulacion

📲 Contacta al GC y ofrece insulacion para el proyecto
```

---

## Mantenimiento

### Ver logs
```bash
# systemd
sudo journalctl -u insulleads -f

# Docker
docker logs -f insulleads

# PM2
pm2 logs insulleads
```

### Actualizar el codigo
```bash
cd /home/insulleads/Insulleads
git pull origin main
sudo systemctl restart insulleads
```

### Backup de la base de datos
```bash
cp data/leads.db data/leads.db.backup.$(date +%Y%m%d)
```

### Resetear leads (re-enviar todos)
```bash
rm data/leads.db
sudo systemctl restart insulleads
```

---

## Presupuesto

| Escenario | Costo/mes | Leads estimados |
|-----------|-----------|-----------------|
| Solo APIs gratuitas | $0 + $6 droplet = **$6** | 200-500 |
| Tier 1 (esencial) | $300 + $6 = **$306** | 500-1,500 |
| Tier 1+2 (crecimiento) | $500 + $6 = **$506** | 1,500-3,000 |
| Full stack (premium) | $1,100 + $6 = **$1,106** | 3,000-5,000+ |

---

## FAQ

**Las APIs de datos abiertos son gratuitas?**
Si. Socrata, CKAN, SeeClickFix, NOAA, NREL, Census — todas son 100% gratuitas.

**Que pasa si una ciudad no tiene API disponible?**
El agente omite esa fuente silenciosamente (`_skip_if_no_data: True`) y sigue con las demas.

**El mismo lead se envia dos veces?**
No. El sistema tiene doble deduplicacion: por agente (SQLite `sent_leads`) y cross-agent (address normalization en `consolidated_leads`).

**Puedo agregar mas ciudades?**
Si. Agrega un nuevo dict a la lista de fuentes del agente correspondiente con la URL y field_map.

**Puedo agregar mas CSVs de contactos?**
Solo copia el archivo a `contacts/` y reinicia. Se carga automaticamente.

**Cuanto RAM necesita?**
~200MB con todos los agentes activos. Un droplet de 1GB es suficiente.

**Funciona sin las APIs de pago?**
Si. El sistema funciona 100% con APIs gratuitas. Las de pago solo enriquecen los datos.
