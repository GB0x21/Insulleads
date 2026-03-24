# 🤖 Insul-Techs Lead Agents

Sistema automatizado de detección de leads para Insul-Techs, Inc.
Monitorea fuentes de datos públicas del Bay Area y envía alertas
directamente a Telegram en tiempo real.

---

## 🧠 Agentes Incluidos

| Agente | Fuente | Intervalo | Por qué genera leads |
|--------|--------|-----------|---------------------|
| 🏗️ Permisos de Construcción | SF DataSF, San Jose, Oakland Open Data | Cada 60 min | ADU/Remodel/Addition = necesitan insulación |
| ☀️ Instalaciones Solares | Permisos solar + Cal. Solar Initiative | Cada 60 min | Solar nuevo = necesitan mejorar aislamiento para maximizar ahorro |
| 🐀 Reportes 311 Roedores | SF 311, Oakland SeeClickFix, SJ 311 | Cada 2 hrs | Roedores = daño a insulación de ático |
| 🌊 Alertas NOAA Inundación | NOAA Weather API | Cada 30 min | Agua/humedad = crawlspace insulation dañada |

---

## ⚡ Instalación Rápida (5 minutos)

### 1. Requisitos
- Python 3.10+
- Cuenta de Telegram

### 2. Clonar e instalar
```bash
git clone https://github.com/tu-usuario/insultechs-agents.git
cd insultechs-agents
pip install -r requirements.txt
```

### 3. Crear tu Bot de Telegram

1. Abre Telegram y busca **@BotFather**
2. Escribe `/newbot`
3. Dale un nombre: `InsulTechs Leads`
4. Dale un username: `insultechs_leads_bot`
5. Copia el **token** que te da

### 4. Obtener tu Chat ID

1. Crea un grupo en Telegram (ej: "🏗️ Insul-Techs Leads")
2. Agrega tu bot al grupo
3. Escribe cualquier mensaje en el grupo
4. Visita: `https://api.telegram.org/bot<TU_TOKEN>/getUpdates`
5. Busca `"chat":{"id":` — ese número es tu Chat ID (normalmente negativo)

### 5. Configurar .env
```bash
cp .env.example .env
nano .env   # o usa tu editor favorito
```

Rellena:
```
TELEGRAM_BOT_TOKEN=123456789:ABCdef...
TELEGRAM_CHAT_ID=-1001234567890
```

### 6. Probar la conexión
```bash
python main.py --test
```
Deberías recibir un mensaje en tu grupo de Telegram. ✅

### 7. ¡Lanzar!
```bash
python main.py
```

---

## 🎮 Comandos Disponibles

```bash
# Iniciar todos los agentes
python main.py

# Probar conexión a Telegram
python main.py --test

# Ejecutar solo un agente (para probar)
python main.py --run permits
python main.py --run solar
python main.py --run rodents
python main.py --run flood

# Ver estadísticas de leads enviados
python main.py --stats
```

---

## 🖥️ Correr en Servidor 24/7

### Opción A — PM2 (recomendado, igual que Node.js)
```bash
npm install -g pm2
pm2 start "python main.py" --name insultechs-agents
pm2 save
pm2 startup
```

### Opción B — systemd (Linux)
```bash
sudo nano /etc/systemd/system/insultechs.service
```

```ini
[Unit]
Description=Insul-Techs Lead Agents
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/insultechs-agents
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable insultechs
sudo systemctl start insultechs
sudo systemctl status insultechs
```

### Opción C — Docker
```bash
# Construir
docker build -t insultechs-agents .

# Correr
docker run -d \
  --name leads \
  --restart always \
  -v $(pwd)/.env:/app/.env \
  -v $(pwd)/data:/app/data \
  insultechs-agents
```

### Opción D — Servidor en la nube (VPS barato)
- **DigitalOcean Droplet** $6/mes — 1GB RAM más que suficiente
- **Railway.app** — Free tier disponible
- **Render.com** — Free tier disponible

---

## 📱 Ejemplo de Mensaje en Telegram

```
🏗️ PERMISOS DE CONSTRUCCIÓN
━━━━━━━━━━━━━━━━━━━━
📌 San Francisco — 1420 Market St

▸ Tipo de Permiso: ADU - Accessory Dwelling Unit
▸ Descripción: Convert existing garage to ADU, add insulation...
▸ Estado: ISSUED
▸ Fecha Solicitud: 2026-03-10
▸ Contratista: ABC Construction Inc
▸ Propietario: John Smith
▸ Valor Estimado: $85,000
▸ Ver Permiso: https://sfdbi.org/permit/202603101234

💡 Contacta al contratista y ofrece insulación para el proyecto

🕐 10/03/2026 09:45
```

---

## 🗂️ Estructura del Proyecto

```
insultechs_agents/
│
├── main.py                  # Orquestador principal
├── requirements.txt         # Dependencias Python
├── .env.example             # Plantilla de configuración
│
├── agents/
│   ├── base.py              # Clase base (herencia)
│   ├── permits_agent.py     # 🏗️ Permisos de construcción
│   ├── solar_agent.py       # ☀️ Instalaciones solares
│   ├── rodents_agent.py     # 🐀 Reportes 311 roedores
│   └── flood_agent.py       # 🌊 Alertas NOAA inundación
│
├── utils/
│   ├── db.py                # SQLite — evita leads duplicados
│   └── telegram.py          # Formatea y envía mensajes
│
└── data/
    └── leads.db             # Base de datos local (auto-creada)
```

---

## 🔧 Agregar Nuevos Agentes

Cualquier fuente de datos nueva se puede agregar en 3 pasos:

```python
# agents/mi_nuevo_agente.py
from agents.base import BaseAgent
from utils.telegram import send_lead

class MiNuevoAgente(BaseAgent):
    name      = "🎯 Mi Nuevo Agente"
    emoji     = "🎯"
    agent_key = "mi_agente"    # clave única

    def fetch_leads(self) -> list[dict]:
        # Llama tu API aquí
        # Retorna lista de dicts, cada uno con 'id' único
        return [{"id": "123", "address": "..."}]

    def notify(self, lead: dict):
        send_lead(
            agent_name=self.name,
            emoji=self.emoji,
            title=lead["address"],
            fields={"Campo 1": lead.get("campo1")},
            cta="Tu call-to-action aquí"
        )
```

Luego regístralo en `main.py`:
```python
AGENTS["mi_agente"] = {
    "class": MiNuevoAgente,
    "env_key": "AGENT_MI_AGENTE",
    "interval": "INTERVAL_MI_AGENTE",
    "default_interval": 60,
}
```

---

## 📊 Base de Datos

Los leads se guardan en `data/leads.db` (SQLite).
Nunca se envía el mismo lead dos veces.

```bash
# Ver todos los leads guardados
sqlite3 data/leads.db "SELECT * FROM sent_leads ORDER BY sent_at DESC LIMIT 20;"

# Ver estadísticas por agente
python main.py --stats
```

---

## ❓ Preguntas Frecuentes

**¿Las APIs son gratuitas?**
Sí, todas son APIs públicas del gobierno. No requieren key ni pago.

**¿Puedo filtrar solo ciertas ciudades?**
Sí, modifica la lista en cada agente o agrega filtros en `.env`.

**¿Qué pasa si una API falla?**
El agente registra el error, te notifica por Telegram y sigue corriendo.
Los otros agentes no se ven afectados.

**¿Puedo agregar más agentes (Redfin, Zillow, etc.)?**
Sí, siguiendo el patrón de `BaseAgent`. Redfin/Zillow requieren
scraping ya que no tienen API pública, pero la estructura es la misma.

---


