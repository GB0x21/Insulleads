#!/bin/bash
#
# setup.sh — Insulleads Complete Setup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Este script instala y configura TODA la infraestructura del proyecto.
#
# Uso:
#   bash setup.sh              # Setup interactivo completo
#   bash setup.sh --skip-venv  # Omitir creación de venv (ya existe)
#   bash setup.sh --ci         # Setup para CI/CD (no interactivo)
#

set -e  # Exit on any error

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# State
SKIP_VENV=false
CI_MODE=false
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-venv) SKIP_VENV=true; shift ;;
        --ci) CI_MODE=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo -e "${BLUE}╔════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     INSULLEADS — COMPLETE SETUP            ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════╝${NC}"
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 1: Check Python version
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[1/9] Checking Python version...${NC}"
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
REQUIRED_VERSION="3.10"
if [[ $(echo -e "$PYTHON_VERSION\n$REQUIRED_VERSION" | sort -V | head -n1) != $REQUIRED_VERSION ]]; then
    echo -e "${RED}✗ Python 3.10+ required, found $PYTHON_VERSION${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python $PYTHON_VERSION${NC}"
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 2: Create virtual environment
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[2/9] Setting up virtual environment...${NC}"
if [ "$SKIP_VENV" = false ]; then
    if [ -d "venv" ]; then
        echo -e "${YELLOW}  venv already exists, skipping creation${NC}"
    else
        python3 -m venv venv
        echo -e "${GREEN}  ✓ venv created${NC}"
    fi
fi

source venv/bin/activate 2>/dev/null || {
    echo -e "${RED}✗ Could not activate venv${NC}"
    exit 1
}
echo -e "${GREEN}✓ Virtual environment activated${NC}"
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 3: Upgrade pip
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[3/9] Upgrading pip...${NC}"
pip install --upgrade pip setuptools wheel > /dev/null 2>&1
echo -e "${GREEN}✓ pip upgraded${NC}"
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 4: Install Python dependencies
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[4/9] Installing Python dependencies from requirements.txt...${NC}"
if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}✗ requirements.txt not found${NC}"
    exit 1
fi
pip install -r requirements.txt
echo -e "${GREEN}✓ Dependencies installed${NC}"
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 5: Install Playwright browsers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[5/9] Installing Playwright browsers...${NC}"
playwright install chromium
echo -e "${GREEN}✓ Playwright browsers installed${NC}"
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 6: Create .env file
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[6/9] Setting up environment file...${NC}"
if [ ! -f ".env" ]; then
    if [ ! -f ".env.example" ]; then
        echo -e "${RED}✗ .env.example not found${NC}"
        exit 1
    fi
    cp .env.example .env
    echo -e "${GREEN}✓ .env created from .env.example${NC}"
    echo -e "${YELLOW}  ⚠ Edit .env with your API keys:${NC}"
    echo -e "${YELLOW}    - TELEGRAM_BOT_TOKEN${NC}"
    echo -e "${YELLOW}    - TELEGRAM_CHAT_ID${NC}"
    echo -e "${YELLOW}    - FIRECRAWL_API_KEY (optional)${NC}"
else
    echo -e "${GREEN}✓ .env already exists${NC}"
fi
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 7: Initialize databases
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[7/9] Initializing SQLite databases...${NC}"
python3 << 'PYEOF'
import sys
try:
    from utils.db import init_db
    init_db()
    print("\033[0;32m✓ Databases initialized\033[0m")
except Exception as e:
    print(f"\033[0;31m✗ Database init failed: {e}\033[0m")
    sys.exit(1)
PYEOF
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 8: Test Telegram connection (if configured)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[8/9] Testing Telegram connection...${NC}"
python3 << 'PYEOF'
import os
from dotenv import load_dotenv
load_dotenv()

token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

if not token or token == "123456789:ABCdef...":
    print("\033[1;33m⚠ TELEGRAM_BOT_TOKEN not configured${NC}")
    print("\033[1;33m  Set it in .env to enable notifications${NC}")
elif not chat_id or chat_id == "-1001234567890":
    print("\033[1;33m⚠ TELEGRAM_CHAT_ID not configured${NC}")
    print("\033[1;33m  Set it in .env to enable notifications${NC}")
else:
    try:
        import requests
        url = f"https://api.telegram.org/bot{token}/getMe"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            print("\033[0;32m✓ Telegram bot configured and accessible\033[0m")
        else:
            print("\033[0;31m✗ Telegram bot token invalid\033[0m")
    except Exception as e:
        print(f"\033[0;31m✗ Telegram connection failed: {e}\033[0m")
PYEOF
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 9: Show next steps
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[9/9] Setup complete!${NC}"
echo
echo -e "${GREEN}╔════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║     INSULLEADS READY TO USE                ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════╝${NC}"
echo
echo -e "${BLUE}Next steps:${NC}"
echo -e "  1. ${YELLOW}Edit .env${NC} with your API keys (if not already done)"
echo -e "  2. ${YELLOW}Test:${NC} python main.py --test"
echo -e "  3. ${YELLOW}Run single agent:${NC} python main.py --run permits"
echo -e "  4. ${YELLOW}Start all agents:${NC} python main.py"
echo
echo -e "${BLUE}Optional:${NC}"
echo -e "  • Setup Krayin CRM: python utils/crm_setup.py"
echo -e "  • View project docs: cat DEPENDENCIES.md"
echo -e "  • View design docs: cat CLAUDE.md"
echo
echo -e "${BLUE}Configuration files:${NC}"
echo -e "  • .env — API keys and settings"
echo -e "  • DEPENDENCIES.md — External repos and APIs"
echo -e "  • CLAUDE.md — Development guidelines"
echo
