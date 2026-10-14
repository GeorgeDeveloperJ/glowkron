#!/bin/sh
set -eu

echo "==> GlowKron Systemd User Service Installer"

# 1. Check for glowkron binary location
TARGET_BIN_DIR="$HOME/.local/bin"
TARGET_BIN="$TARGET_BIN_DIR/glowkron"
FOUND_BIN=""

if [ -x "$TARGET_BIN" ]; then
    FOUND_BIN="$TARGET_BIN"
elif [ -n "${GOPATH:-}" ] && [ -x "$GOPATH/bin/glowkron" ]; then
    FOUND_BIN="$GOPATH/bin/glowkron"
elif command -v go >/dev/null 2>&1 && [ -x "$(go env GOPATH 2>/dev/null)/bin/glowkron" ]; then
    FOUND_BIN="$(go env GOPATH)/bin/glowkron"
elif command -v glowkron >/dev/null 2>&1; then
    FOUND_BIN="$(command -v glowkron)"
elif [ -x "./glowkron" ]; then
    FOUND_BIN="./glowkron"
fi

if [ -z "$FOUND_BIN" ]; then
    echo "WARNING: 'glowkron' binary not found in $TARGET_BIN_DIR, \$GOPATH/bin, or \$PATH."
    echo "Make sure to build and install it to $TARGET_BIN (e.g. 'make build && mkdir -p $TARGET_BIN_DIR && cp ./glowkron $TARGET_BIN_DIR/')."
else
    echo "Found glowkron binary at: $FOUND_BIN"
    if [ "$FOUND_BIN" != "$TARGET_BIN" ]; then
        echo "Ensuring binary is installed at $TARGET_BIN (matching ExecStart in glowkron.service)..."
        mkdir -p "$TARGET_BIN_DIR"
        cp "$FOUND_BIN" "$TARGET_BIN"
        chmod +x "$TARGET_BIN"
        echo "Successfully copied $FOUND_BIN to $TARGET_BIN"
    fi
fi

# 2. Setup user systemd directory
SYSTEMD_USER_DIR="$HOME/.config/systemd/user"
mkdir -p "$SYSTEMD_USER_DIR"

# 3. Copy systemd service file
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_SOURCE="$SCRIPT_DIR/glowkron.service"

if [ ! -f "$SERVICE_SOURCE" ]; then
    echo "ERROR: Service template file not found at $SERVICE_SOURCE" >&2
    exit 1
fi

echo "Copying glowkron.service to $SYSTEMD_USER_DIR/..."
cp "$SERVICE_SOURCE" "$SYSTEMD_USER_DIR/glowkron.service"

# 4. Reload user systemd daemon if systemctl is available
if command -v systemctl >/dev/null 2>&1; then
    echo "Reloading systemd user daemon..."
    if systemctl --user daemon-reload 2>/dev/null; then
        echo "systemd user daemon reloaded successfully."
    else
        echo "Notice: Could not connect to systemd user bus (session inactive or headless environment)."
        echo "Run 'systemctl --user daemon-reload' once your user session is active."
    fi
else
    echo "Notice: systemctl command not found; skipping daemon-reload."
fi

# 5. Print status and next steps
echo ""
echo "==> GlowKron service installed successfully!"
echo ""
echo "To enable and start the GlowKron background service now:"
echo "    systemctl --user enable --now glowkron"
echo ""
echo "To check service status and logs:"
echo "    systemctl --user status glowkron"
echo "    journalctl --user -u glowkron -f"
echo ""
echo "To ensure GlowKron runs persistently even when logged out (headless daemon):"
echo "    loginctl enable-linger $USER"
echo ""
