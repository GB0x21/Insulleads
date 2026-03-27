# 🤖 Insul Lead Agents

Sistema automatizado de detección de leads para **Insulation**
Monitorea fuentes de datos públicas del **Bay Area completo** y envía alertas
directamente a Telegram en tiempo real — incluyendo los datos de contacto del GC.

---

## 🧠 Agentes Incluidos

| Agente | Fuente | Intervalo | Por qué genera leads |
|--------|--------|-----------|----------------------|
| 🏗️ Permisos de Construcción | SF · Oakland · SJ · Berkeley · Sunnyvale · Santa Clara · Richmond · Fremont · Hayward | Cada 60 min | ADU/Remodel/Addition = necesitan insulación |
| ☀️ Instalaciones Solares | Permisos solar SF + Oakland | Cada 60 min | Solar nuevo = necesitan mejorar aislamiento |
| 🐀 Reportes 311 Roedores | SF 311 · Oakland SeeClickFix | Cada 2 hrs | Roedores = daño a insulación de ático |
| 🌊 Alertas NOAA Inundación | NOAA Weather API (5 zonas Bay Area) | Cada 30 min | Agua = crawlspace insulation dañada |

---

## 📞 Enriquecimiento de Contacto GC

Para los permisos de construcción, el bot busca automáticamente el teléfono y email
del contratista general (GC) en este orden:

1. **CSVs locales** en `contacts/` — fuzzy matching por nombre de empresa
2. **CSLB** (California Contractors State License Board) — fallback web

El mensaje de Telegram incluye: teléfono, email, nombre exacto del GC y la fuente del dato.

---

## ⚡ Instalación (5 minutos)

### 1. Clonar e instalar

```bash
git clone https://github.com/tu-usuario/insulleads.git
cd insulleads
pip install -r requirements.txt
```

### 2. Configurar Telegram

1. Abre Telegram → busca **@BotFather** → `/newbot`
2. Copia el **token** que te da
3. Crea un grupo, agrega tu bot, escribe algo
4. Visita `https://api.telegram.org/bot<TOKEN>/getUpdates`
5. Busca `"chat":{"id":` — ese es tu Chat ID (número negativo)

### 3. Crear `.env`

```bash
cp .env.example .env
```

Edita `.env` y rellena:
```
TELEGRAM_BOT_TOKEN=123456789:ABCdef...
TELEGRAM_CHAT_ID=-1001234567890
```

### 4. Agregar tus contactos GC

Copia tus archivos `.csv` a la carpeta `contacts/`.
Ver `contacts/README.md` para formatos soportados.

### 5. Probar

```bash
python main.py --test
```

### 6. ¡Lanzar!

```bash
python main.py
```

---

## 🎮 Comandos

```bash
python main.py               # Inicia todos los agentes
python main.py --test        # Prueba conexión Telegram
python main.py --run permits # Ejecuta solo permisos
python main.py --run solar   # Ejecuta solo solar
python main.py --run rodents # Ejecuta solo roedores
python main.py --run flood   # Ejecuta solo inundaciones
python main.py --stats       # Ve estadísticas de leads enviados
```

---

## 📱 Ejemplo de Mensaje en Telegram

```
🏗️ PERMISOS DE CONSTRUCCIÓN — BAY AREA
━━━━━━━━━━━━━━━━━━━━
📌 Oakland — 4521 Broadway Ave

▸ Ciudad: Oakland
▸ Tipo de Permiso: REMODEL
▸ Descripción: Kitchen remodel, new insulation in walls and attic...
▸ Estado: ISSUED
▸ Fecha Solicitud: 2026-03-10
▸ Fecha Emisión: 2026-03-18
▸ Contratista (GC): KAUFMAN HOME IMPROVEMENT
▸ Licencia CSLB: 876543
▸ Teléfono GC: +19253820739  (via CSV B_CONTACTS_GC.csv)
▸ Email GC: info@kaufmanhome.com
▸ Propietario: Maria Rodriguez
▸ Valor Estimado: $42,000

💡 Contacta al GC y ofrece insulación para el proyecto

🕐 26/03/2026 09:45
```

---

## 🗂️ Estructura del Proyecto

```
insulleads/
├── main.py                    # Orquestador principal
├── requirements.txt
├── .env.example
├── .gitignore
├── Dockerfile
│
├── agents/
│   ├── base.py               # Clase base (deduplicación)
│   ├── permits_agent.py      # 🏗️ Permisos — Bay Area completa
│   ├── solar_agent.py        # ☀️ Instalaciones solares
│   ├── rodents_agent.py      # 🐀 Reportes 311 roedores
│   └── flood_agent.py        # 🌊 Alertas NOAA inundación
│
├── utils/
│   ├── db.py                 # SQLite — evita leads duplicados
│   ├── telegram.py           # Formatea y envía mensajes
│   └── contacts_loader.py    # Carga y unifica CSVs de contactos
│
├── contacts/                 # ← PON AQUÍ TUS CSVs DE GCs
│   └── README.md             # Formatos soportados
│
└── data/
    └── leads.db              # Auto-creada al iniciar
```

---

## 🖥️ Correr 24/7

### Docker (recomendado)

```bash
docker build -t insulleads .
docker run -d \
  --name leads \
  --restart always \
  -v $(pwd)/.env:/app/.env \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/contacts:/app/contacts \
  insulleads
```

### PM2

```bash
npm install -g pm2
pm2 start "python main.py" --name insulleads
pm2 save && pm2 startup
```

### systemd

```ini
[Unit]
Description=Insul-lations Lead Agents
After=network.target

[Service]
WorkingDirectory=/home/ubuntu/insulleads
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

---

## ❓ FAQ

**¿Las APIs de permisos son gratuitas?**
Sí, todas son APIs públicas de datos abiertos del gobierno. Sin costo ni registro.

**¿Qué pasa si una ciudad no tiene API disponible?**
El agente omite esa ciudad silenciosamente y sigue con las demás.

**¿Puedo agregar más ciudades?**
Sí, agrega un nuevo dict a `PERMIT_SOURCES` en `permits_agent.py` con la URL y el mapeo de campos.

**¿Puedo agregar más CSVs de contactos?**
Solo copia el archivo a `contacts/` — se carga automáticamente.

**¿El mismo lead se envía dos veces?**
No. Todos los leads se registran en `data/leads.db` y nunca se repiten.
