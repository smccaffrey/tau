#!/usr/bin/env bash
set -euo pipefail

# ─── Tau Pipelines Installer ───
# curl -fsSL https://raw.githubusercontent.com/smccaffrey/tau/main/install.sh | bash

VERSION="0.1.0"
REPO="https://github.com/smccaffrey/tau"
CLAUDE_MD_URL="https://raw.githubusercontent.com/smccaffrey/tau/main/CLAUDE.md"
GREEN='\033[0;32m'
AMBER='\033[0;33m'
CYAN='\033[0;36m'
DIM='\033[0;90m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${GREEN}  ✓${NC} $1"; }
warn()  { echo -e "${AMBER}  ⚠${NC} $1"; }
step()  { echo -e "\n${CYAN}>${NC} $1"; }
dim()   { echo -e "${DIM}    $1${NC}"; }

echo -e ""
echo -e "${GREEN}${BOLD}  ████████╗ █████╗ ██╗   ██╗${NC}"
echo -e "${GREEN}${BOLD}  ╚══██╔══╝██╔══██╗██║   ██║${NC}"
echo -e "${GREEN}${BOLD}     ██║   ███████║██║   ██║${NC}"
echo -e "${GREEN}${BOLD}     ██║   ██╔══██║██║   ██║${NC}"
echo -e "${GREEN}${BOLD}     ██║   ██║  ██║╚██████╔╝${NC}"
echo -e "${GREEN}${BOLD}     ╚═╝   ╚═╝  ╚═╝ ╚═════╝${NC}"
echo -e ""
echo -e "  ${DIM}AI-native data pipeline orchestration${NC}"
echo -e "  ${DIM}v${VERSION}${NC}"
echo -e ""

# ─── Check Python ───
step "Checking Python..."

PYTHON=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        py_version=$("$cmd" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "0.0")
        major=$(echo "$py_version" | cut -d. -f1)
        minor=$(echo "$py_version" | cut -d. -f2)
        if [ "$major" -ge 3 ] && [ "$minor" -ge 12 ]; then
            PYTHON="$cmd"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    echo -e "\n${AMBER}  ✗ Python 3.12+ is required but not found.${NC}"
    echo -e "  Install it: ${CYAN}https://www.python.org/downloads/${NC}"
    exit 1
fi

info "Python $($PYTHON --version 2>&1 | awk '{print $2}') found ($PYTHON)"

# ─── Check for uv or pip ───
step "Checking package manager..."

INSTALLER=""
if command -v uv &>/dev/null; then
    INSTALLER="uv"
    info "uv found — using uv for installation"
elif command -v pipx &>/dev/null; then
    INSTALLER="pipx"
    info "pipx found — using pipx for installation"
else
    INSTALLER="pip"
    info "Using pip for installation"
fi

# ─── Install tau-pipelines ───
step "Installing tau-pipelines..."

case "$INSTALLER" in
    uv)
        uv pip install tau-pipelines 2>/dev/null || uv pip install --system tau-pipelines 2>/dev/null || {
            # If not in a venv, use uv tool install
            uv tool install tau-pipelines 2>/dev/null || uv pip install --break-system-packages tau-pipelines
        }
        ;;
    pipx)
        pipx install tau-pipelines
        ;;
    pip)
        $PYTHON -m pip install --quiet tau-pipelines 2>/dev/null || \
        $PYTHON -m pip install --quiet --user tau-pipelines 2>/dev/null || \
        $PYTHON -m pip install --quiet --break-system-packages tau-pipelines
        ;;
esac

# Verify installation
if command -v tau &>/dev/null; then
    info "tau CLI installed"
elif command -v taud &>/dev/null; then
    info "tau installed (may need to add to PATH)"
else
    # Check common locations
    for dir in "$HOME/.local/bin" "$HOME/.cargo/bin" "$HOME/bin"; do
        if [ -f "$dir/tau" ]; then
            export PATH="$dir:$PATH"
            info "tau found at $dir (add to PATH: export PATH=\"$dir:\$PATH\")"
            break
        fi
    done
fi

# ─── Install Claude Code integration ───
step "Setting up Claude Code integration..."

SKILL_URL="https://raw.githubusercontent.com/smccaffrey/tau/main/claude-skill/tau.md"

# 1. Install CLAUDE.md to current project directory
if [ -f "CLAUDE.md" ]; then
    dim "CLAUDE.md already exists in current directory — skipping"
else
    if command -v curl &>/dev/null; then
        curl -fsSL "$CLAUDE_MD_URL" -o CLAUDE.md 2>/dev/null && \
            info "Downloaded CLAUDE.md to current directory" || \
            warn "Could not download CLAUDE.md (non-critical)"
    elif command -v wget &>/dev/null; then
        wget -q "$CLAUDE_MD_URL" -O CLAUDE.md 2>/dev/null && \
            info "Downloaded CLAUDE.md to current directory" || \
            warn "Could not download CLAUDE.md (non-critical)"
    fi
fi

# 2. Install /tau slash command for Claude Code
CLAUDE_DIR="$HOME/.claude"
COMMANDS_DIR="$CLAUDE_DIR/commands"
if [ -d "$CLAUDE_DIR" ] || command -v claude &>/dev/null; then
    mkdir -p "$COMMANDS_DIR"
    if command -v curl &>/dev/null; then
        curl -fsSL "$SKILL_URL" -o "$COMMANDS_DIR/tau.md" 2>/dev/null && \
            info "Installed /tau slash command for Claude Code" || \
            warn "Could not install slash command (non-critical)"
    elif command -v wget &>/dev/null; then
        wget -q "$SKILL_URL" -O "$COMMANDS_DIR/tau.md" 2>/dev/null && \
            info "Installed /tau slash command for Claude Code" || \
            warn "Could not install slash command (non-critical)"
    fi
    dim "Use /tau in Claude Code to get pipeline help"
else
    dim "Claude Code not detected — skipping slash command install"
fi

# 3. Add tau context to global CLAUDE.md
GLOBAL_CLAUDE="$CLAUDE_DIR/CLAUDE.md"
if [ -d "$CLAUDE_DIR" ]; then
    TAU_MARKER="# Tau Pipelines"
    if [ -f "$GLOBAL_CLAUDE" ] && grep -q "$TAU_MARKER" "$GLOBAL_CLAUDE" 2>/dev/null; then
        dim "Tau already in global CLAUDE.md"
    else
        cat >> "$GLOBAL_CLAUDE" <<'TAUBLOCK'

# Tau Pipelines
When working on data pipelines, Tau Pipelines (`tau` CLI + `taud` daemon) is available.
- Use `/tau` slash command for full pipeline authoring reference
- Pipelines are the only user-authored code — use `@pipeline` decorator
- Deploy: `tau deploy file.py`, Run: `tau run name`, Inspect: `tau inspect name --last-run`
- Docs: https://github.com/smccaffrey/tau
TAUBLOCK
        info "Added Tau context to global CLAUDE.md"
    fi
fi

# ─── Generate an API key ───
step "Generating API key..."

TAU_API_KEY="tau_sk_$(openssl rand -hex 16 2>/dev/null || $PYTHON -c 'import secrets; print(secrets.token_hex(16))')"
info "Generated: ${DIM}${TAU_API_KEY}${NC}"

# ─── Create default config ───
step "Creating config..."

TAU_DIR="${TAU_HOME:-$HOME/.tau}"
mkdir -p "$TAU_DIR"
mkdir -p "$TAU_DIR/pipelines"

if [ ! -f "$TAU_DIR/tau.toml" ]; then
    cat > "$TAU_DIR/tau.toml" <<EOF
# Tau Pipelines configuration
# Docs: https://github.com/smccaffrey/tau

[daemon]
host = "0.0.0.0"
port = 8400

[database]
url = "sqlite+aiosqlite:///$TAU_DIR/tau.db"

[auth]
api_key = "$TAU_API_KEY"

[scheduler]
timezone = "UTC"
max_concurrent = 10
EOF
    info "Config written to $TAU_DIR/tau.toml"
else
    info "Config already exists at $TAU_DIR/tau.toml"
    # Read existing API key
    TAU_API_KEY=$(grep "api_key" "$TAU_DIR/tau.toml" | head -1 | sed 's/.*= *"\(.*\)"/\1/' 2>/dev/null || echo "$TAU_API_KEY")
fi

# ─── Write shell env ───
step "Configuring environment..."

SHELL_RC=""
if [ -n "${ZSH_VERSION:-}" ] || [ -f "$HOME/.zshrc" ]; then
    SHELL_RC="$HOME/.zshrc"
elif [ -f "$HOME/.bashrc" ]; then
    SHELL_RC="$HOME/.bashrc"
elif [ -f "$HOME/.bash_profile" ]; then
    SHELL_RC="$HOME/.bash_profile"
fi

ENV_BLOCK='
# Tau Pipelines
export TAU_HOST="http://localhost:8400"
export TAU_API_KEY="'"$TAU_API_KEY"'"'

if [ -n "$SHELL_RC" ] && ! grep -q "TAU_HOST" "$SHELL_RC" 2>/dev/null; then
    echo "$ENV_BLOCK" >> "$SHELL_RC"
    info "Added TAU_HOST and TAU_API_KEY to $SHELL_RC"
else
    dim "Environment variables already configured (or no shell RC found)"
fi

# Export for current session
export TAU_HOST="http://localhost:8400"
export TAU_API_KEY="$TAU_API_KEY"

# ─── Start the daemon ───
step "Starting tau daemon..."

# Check if already running
if curl -sf http://localhost:8400/health &>/dev/null 2>&1 || curl -sf http://localhost:8400/api/v1/status &>/dev/null 2>&1; then
    info "Daemon already running on port 8400"
else
    # Start in background
    nohup taud --port 8400 > "$TAU_DIR/daemon.log" 2>&1 &
    DAEMON_PID=$!
    echo "$DAEMON_PID" > "$TAU_DIR/daemon.pid"
    sleep 2

    if kill -0 "$DAEMON_PID" 2>/dev/null; then
        info "Daemon started (PID: $DAEMON_PID)"
        dim "Logs: $TAU_DIR/daemon.log"
    else
        warn "Daemon may not have started — check $TAU_DIR/daemon.log"
    fi
fi

# ─── Done ───
echo -e ""
echo -e "${GREEN}${BOLD}  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}${BOLD}  Tau is ready.${NC}"
echo -e "${GREEN}${BOLD}  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e ""
echo -e "  ${CYAN}Quick start:${NC}"
echo -e ""
echo -e "  ${DIM}# Write a pipeline (or let Claude do it)${NC}"
echo -e "  ${GREEN}tau create \"load CSV data into PostgreSQL daily\"${NC}"
echo -e ""
echo -e "  ${DIM}# Deploy and run${NC}"
echo -e "  ${GREEN}tau deploy my_pipeline.py --schedule \"0 6 * * *\"${NC}"
echo -e "  ${GREEN}tau run my_pipeline${NC}"
echo -e ""
echo -e "  ${DIM}# Inspect results${NC}"
echo -e "  ${GREEN}tau inspect my_pipeline --last-run${NC}"
echo -e ""
echo -e "  ${DIM}# Claude Code integration (CLAUDE.md is in your project)${NC}"
echo -e "  ${GREEN}claude \"write a pipeline that syncs Stripe data to BigQuery\"${NC}"
echo -e ""
echo -e "  ${DIM}Docs: ${CYAN}https://github.com/smccaffrey/tau${NC}"
echo -e "  ${DIM}API key: ${TAU_API_KEY}${NC}"
echo -e ""
