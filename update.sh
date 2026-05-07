#!/bin/bash
#
# update.sh — Insulleads Update & Install Missing Dependencies
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Actualiza el repo desde git e instala solo lo que hace falta.
#
# Uso:
#   bash update.sh              # Update automático + install missing
#   bash update.sh --skip-git   # Solo instala, no hace git pull
#   bash update.sh --dry-run    # Muestra qué haría sin hacerlo
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# State
SKIP_GIT=false
DRY_RUN=false
VENV_ACTIVE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-git) SKIP_GIT=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo -e "${BLUE}╔════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   INSULLEADS UPDATE & INSTALL MISSING      ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════╝${NC}"
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 1: Check if venv is active
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[1/8] Checking virtual environment...${NC}"
if [[ -z "$VIRTUAL_ENV" ]]; then
    if [ -d "venv" ]; then
        echo -e "${YELLOW}  venv exists but not activated, activating...${NC}"
        source venv/bin/activate
        VENV_ACTIVE=true
    else
        echo -e "${RED}✗ venv not found. Run: bash setup.sh${NC}"
        exit 1
    fi
else
    VENV_ACTIVE=true
    echo -e "${GREEN}✓ venv already active${NC}"
fi
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 2: Git pull (update repo)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[2/8] Updating from git repository...${NC}"
if [ "$SKIP_GIT" = true ]; then
    echo -e "${YELLOW}  Skipping git pull (--skip-git)${NC}"
else
    if [ "$DRY_RUN" = true ]; then
        echo -e "${YELLOW}  [DRY-RUN] Would run: git pull origin claude/review-agents-w702R${NC}"
    else
        git pull origin claude/review-agents-w702R || {
            echo -e "${RED}✗ git pull failed${NC}"
            echo -e "${YELLOW}  Try: git status${NC}"
            exit 1
        }
        echo -e "${GREEN}✓ Repository updated${NC}"
    fi
fi
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 3: Check if requirements.txt changed
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[3/8] Checking if requirements.txt changed...${NC}"
if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}✗ requirements.txt not found${NC}"
    exit 1
fi

NEEDS_PIP_UPDATE=false
if [ -f ".requirements-hash" ]; then
    OLD_HASH=$(cat .requirements-hash)
    NEW_HASH=$(sha256sum requirements.txt | awk '{print $1}')
    if [ "$OLD_HASH" != "$NEW_HASH" ]; then
        NEEDS_PIP_UPDATE=true
        echo -e "${YELLOW}  requirements.txt changed, will update${NC}"
    else
        echo -e "${GREEN}✓ requirements.txt unchanged${NC}"
    fi
else
    NEEDS_PIP_UPDATE=true
    echo -e "${YELLOW}  First time checking, will verify pip packages${NC}"
fi
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 4: Install/update pip packages if needed
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[4/8] Checking Python packages...${NC}"
if [ "$NEEDS_PIP_UPDATE" = true ]; then
    if [ "$DRY_RUN" = true ]; then
        echo -e "${YELLOW}  [DRY-RUN] Would run: pip install -r requirements.txt${NC}"
    else
        pip install -r requirements.txt --upgrade
        sha256sum requirements.txt | awk '{print $1}' > .requirements-hash
        echo -e "${GREEN}✓ Python packages updated${NC}"
    fi
else
    # Verify existing packages even if unchanged
    echo -e "${YELLOW}  Verifying installed packages...${NC}"
    pip check > /dev/null 2>&1 && echo -e "${GREEN}✓ All packages OK${NC}" || {
        echo -e "${YELLOW}  Some packages missing, installing...${NC}"
        pip install -r requirements.txt
        echo -e "${GREEN}✓ Packages fixed${NC}"
    }
fi
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 5: Check Playwright browsers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[5/8] Checking Playwright browsers...${NC}"
PLAYWRIGHT_MISSING=false
if ! python -c "import playwright; from playwright.sync_api import sync_playwright" 2>/dev/null; then
    PLAYWRIGHT_MISSING=true
fi

if [ "$PLAYWRIGHT_MISSING" = true ]; then
    if [ "$DRY_RUN" = true ]; then
        echo -e "${YELLOW}  [DRY-RUN] Would run: playwright install chromium${NC}"
    else
        echo -e "${YELLOW}  Installing Playwright browsers...${NC}"
        playwright install chromium
        echo -e "${GREEN}✓ Playwright browsers installed${NC}"
    fi
else
    echo -e "${GREEN}✓ Playwright browsers already installed${NC}"
fi
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 6: Check .env file
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[6/8] Checking configuration (.env)...${NC}"
if [ ! -f ".env" ]; then
    if [ ! -f ".env.example" ]; then
        echo -e "${RED}✗ .env.example not found${NC}"
        exit 1
    fi
    echo -e "${YELLOW}  Creating .env from .env.example...${NC}"
    cp .env.example .env
    echo -e "${YELLOW}  ⚠ Edit .env with your API keys${NC}"
else
    echo -e "${GREEN}✓ .env already exists (preserved)${NC}"
fi
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 7: Update databases
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[7/8] Updating databases (if needed)...${NC}"
if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}  [DRY-RUN] Would run: python -c 'from utils.db import init_db; init_db()'${NC}"
else
    python << 'PYEOF'
import sys
try:
    from utils.db import init_db
    init_db()
    print("\033[0;32m✓ Databases up to date\033[0m")
except Exception as e:
    print(f"\033[0;31m✗ Database update failed: {e}\033[0m")
    sys.exit(1)
PYEOF
fi
echo

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 8: Summary
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo -e "${YELLOW}[8/8] Summary${NC}"
if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}DRY-RUN MODE: No changes were made${NC}"
    echo -e "${YELLOW}Run without --dry-run to apply updates${NC}"
else
    echo -e "${GREEN}✓ Update complete!${NC}"
fi
echo

echo -e "${GREEN}╔════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║     INSULLEADS READY                       ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════╝${NC}"
echo
echo -e "${BLUE}Next steps:${NC}"
echo -e "  • Test: ${YELLOW}python main.py --test${NC}"
echo -e "  • Run agent: ${YELLOW}python main.py --run permits${NC}"
echo -e "  • Start all: ${YELLOW}python main.py${NC}"
echo

if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}To apply these changes, run:${NC}"
    echo -e "  ${YELLOW}bash update.sh${NC}"
fi
