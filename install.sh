#!/usr/bin/env bash
set -euo pipefail

# ─── Tau Pipelines Installer ───
# curl -fsSL https://raw.githubusercontent.com/smccaffrey/tau/main/install.sh | bash

VERSION="0.1.0"
REPO="https://github.com/smccaffrey/tau"
RAW_URL="https://raw.githubusercontent.com/smccaffrey/tau/main"
GREEN='\033[0;32m'
AMBER='\033[0;33m'
CYAN='\033[0;36m'
DIM='\033[0;90m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${GREEN}  ✓${NC} $1"; }
warn()  { echo -e "${AMBER}  ⚠${NC} $1"; }
err()   { echo -e "${AMBER}  ✗${NC} $1"; }
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
echo -e "  ${DIM}An embedded data orchestrator for AI systems${NC}"
echo -e "  ${DIM}v${VERSION}${NC}"
echo -e ""

# ─── Create ~/.tau directory structure ───
TAU_DIR="${TAU_HOME:-$HOME/.tau}"
mkdir -p "$TAU_DIR"
mkdir -p "$TAU_DIR/pipelines"

# ─── Ensure uv is installed ───
step "Checking for uv..."

if ! command -v uv &>/dev/null; then
    info "Installing uv (fast Python package manager)..."
    curl -LsSf https://astral.sh/uv/install.sh | sh 2>/dev/null
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
    if ! command -v uv &>/dev/null; then
        err "Failed to install uv. Install manually: ${CYAN}https://docs.astral.sh/uv/${NC}"
        exit 1
    fi
fi
info "uv $(uv --version 2>/dev/null | awk '{print $2}') found"

# ─── Ensure Python 3.12+ via uv ───
step "Checking Python 3.12+..."

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
    info "Python 3.12+ not found — installing via uv..."
    uv python install 3.12 2>/dev/null
    info "Python 3.12 installed via uv"
else
    info "Python $($PYTHON --version 2>&1 | awk '{print $2}') found"
fi

# ─── Install tau-pipelines ───
step "Installing tau-pipelines..."

# Install from GitHub repo (not yet published to PyPI)
INSTALL_SRC="tau-pipelines @ git+${REPO}.git"

# Use uv tool install — creates an isolated venv with the right Python automatically
# This handles Python version, venv creation, and PATH in one shot
uv tool install --python ">=3.12" "$INSTALL_SRC" 2>/dev/null || \
uv tool install --python ">=3.12" "$INSTALL_SRC" --force 2>/dev/null || {
    # Fallback: try pip in a uv-managed venv
    warn "uv tool install failed — trying pip fallback..."
    if [ -n "$PYTHON" ]; then
        $PYTHON -m pip install --quiet --user "tau-pipelines @ git+${REPO}.git" 2>/dev/null || \
        $PYTHON -m pip install --quiet "tau-pipelines @ git+${REPO}.git"
    else
        uv pip install --python ">=3.12" --system "tau-pipelines @ git+${REPO}.git" 2>/dev/null || {
            err "Installation failed. Try manually:"
            echo -e "  ${CYAN}uv tool install --python '>=3.12' 'tau-pipelines @ git+${REPO}.git'${NC}"
            exit 1
        }
    fi
}

# Ensure uv tool bin is on PATH
export PATH="$HOME/.local/bin:$PATH"

# Verify installation
if command -v tau &>/dev/null; then
    info "tau CLI installed ($(tau version 2>/dev/null || echo 'ok'))"
elif command -v taud &>/dev/null; then
    info "tau installed (may need to add to PATH)"
else
    warn "tau not found on PATH — add ~/.local/bin to your PATH"
    dim "export PATH=\"\$HOME/.local/bin:\$PATH\""
fi

# ─── Generate API key ───
step "Generating API key..."

if [ -f "$TAU_DIR/tau.toml" ]; then
    # Read existing API key from config
    TAU_API_KEY=$(grep "api_key" "$TAU_DIR/tau.toml" | head -1 | sed 's/.*= *"\(.*\)"/\1/' 2>/dev/null || echo "")
fi

if [ -z "${TAU_API_KEY:-}" ]; then
    TAU_API_KEY="tau_sk_$(openssl rand -hex 16 2>/dev/null || python3 -c 'import secrets; print(secrets.token_hex(16))' 2>/dev/null || uv run python -c 'import secrets; print(secrets.token_hex(16))')"
    info "Generated: ${DIM}${TAU_API_KEY}${NC}"
else
    info "Using existing key from $TAU_DIR/tau.toml"
fi

# ─── Create config ───
step "Creating config..."

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
fi

# ─── Write env file (single source of truth for exports) ───
cat > "$TAU_DIR/env.sh" <<EOF
# Tau Pipelines environment — sourced by shell RC
# Regenerated by installer; edit tau.toml for config changes
export PATH="\$HOME/.local/bin:\$PATH"
export TAU_HOME="$TAU_DIR"
export TAU_HOST="http://localhost:8400"
export TAU_API_KEY="$TAU_API_KEY"
EOF
info "Environment written to $TAU_DIR/env.sh"

# ─── Download Claude Code integration files into ~/.tau ───
step "Setting up Claude Code integration..."

# Helper: download a file into ~/.tau, then symlink to a target
download_to_tau() {
    local filename="$1"
    local url="$2"
    local dest="$TAU_DIR/$filename"

    if command -v curl &>/dev/null; then
        curl -fsSL "$url" -o "$dest" 2>/dev/null && return 0
    elif command -v wget &>/dev/null; then
        wget -q "$url" -O "$dest" 2>/dev/null && return 0
    fi
    return 1
}

# 1. Download CLAUDE.md to ~/.tau/
if download_to_tau "CLAUDE.md" "$RAW_URL/CLAUDE.md"; then
    info "Downloaded CLAUDE.md to $TAU_DIR/CLAUDE.md"
else
    warn "Could not download CLAUDE.md (non-critical)"
fi

# 2. Download /tau slash command to ~/.tau/
if download_to_tau "tau.md" "$RAW_URL/claude-skill/tau.md"; then
    info "Downloaded tau.md to $TAU_DIR/tau.md"
else
    warn "Could not download tau.md (non-critical)"
fi

# 3. Symlink into Claude Code directories if Claude Code is installed
CLAUDE_DIR="$HOME/.claude"
if [ -d "$CLAUDE_DIR" ] || command -v claude &>/dev/null; then
    # Symlink /tau slash command
    COMMANDS_DIR="$CLAUDE_DIR/commands"
    mkdir -p "$COMMANDS_DIR"
    if [ -f "$TAU_DIR/tau.md" ]; then
        ln -sf "$TAU_DIR/tau.md" "$COMMANDS_DIR/tau.md"
        info "Linked /tau slash command → $TAU_DIR/tau.md"
    fi

    # Add tau context to global CLAUDE.md
    GLOBAL_CLAUDE="$CLAUDE_DIR/CLAUDE.md"
    TAU_MARKER="# Tau Pipelines"
    if [ -f "$GLOBAL_CLAUDE" ] && grep -q "$TAU_MARKER" "$GLOBAL_CLAUDE" 2>/dev/null; then
        dim "Tau already in global CLAUDE.md"
    else
        cat >> "$GLOBAL_CLAUDE" <<TAUBLOCK

# Tau Pipelines
When working on data pipelines, Tau Pipelines (\`tau\` CLI + \`taud\` daemon) is available.
- Use \`/tau\` slash command for full pipeline authoring reference
- Pipelines are the only user-authored code — use \`@pipeline\` decorator
- Deploy: \`tau deploy file.py\`, Run: \`tau run name\`, Inspect: \`tau inspect name --last-run\`
- Docs: https://github.com/smccaffrey/tau
TAUBLOCK
        info "Added Tau context to global CLAUDE.md"
    fi
    dim "Use /tau in Claude Code to get pipeline help"
else
    dim "Claude Code not detected — skipping slash command setup"
fi

# ─── Configure shell RC ───
step "Configuring shell..."

SHELL_RC=""
if [ -n "${ZSH_VERSION:-}" ] || [ -f "$HOME/.zshrc" ]; then
    SHELL_RC="$HOME/.zshrc"
elif [ -f "$HOME/.bashrc" ]; then
    SHELL_RC="$HOME/.bashrc"
elif [ -f "$HOME/.bash_profile" ]; then
    SHELL_RC="$HOME/.bash_profile"
fi

TAU_SOURCE_LINE="source \"$TAU_DIR/env.sh\""

if [ -n "$SHELL_RC" ] && ! grep -q "tau/env.sh" "$SHELL_RC" 2>/dev/null; then
    # Remove any old inlined TAU_HOST/TAU_API_KEY exports
    if grep -q "# Tau Pipelines" "$SHELL_RC" 2>/dev/null; then
        # Clean up old-style inline block (best-effort)
        sed -i.bak '/# Tau Pipelines/,/TAU_API_KEY/d' "$SHELL_RC" 2>/dev/null || true
        rm -f "${SHELL_RC}.bak" 2>/dev/null || true
    fi
    cat >> "$SHELL_RC" <<EOF

# Tau Pipelines
[ -f "$TAU_DIR/env.sh" ] && source "$TAU_DIR/env.sh"
EOF
    info "Added source line to $SHELL_RC"
else
    dim "Shell already configured (or no shell RC found)"
fi

# Export for current session
source "$TAU_DIR/env.sh"

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
echo -e "  ${DIM}Everything lives in ${CYAN}~/.tau/${NC}"
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
echo -e "  ${DIM}# Claude Code integration${NC}"
echo -e "  ${GREEN}claude \"write a pipeline that syncs Stripe data to BigQuery\"${NC}"
echo -e ""
echo -e "  ${DIM}Docs: ${CYAN}https://github.com/smccaffrey/tau${NC}"
echo -e ""
