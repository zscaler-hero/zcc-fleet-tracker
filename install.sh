#!/usr/bin/env bash
# ZCC Fleet Tracker — Environment Check
set -e

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok() { echo -e "  ${GREEN}✓${NC} $1"; }
warn() { echo -e "  ${YELLOW}!${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }

echo ""
echo "  ZCC Fleet Tracker — Environment Check"
echo "  ======================================"
echo ""

# Python 3.8+
if command -v python3 &>/dev/null; then
  PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
  PY_MAJ=$(echo "$PY_VER" | cut -d. -f1)
  PY_MIN=$(echo "$PY_VER" | cut -d. -f2)
  if [ "$PY_MAJ" -ge 3 ] && [ "$PY_MIN" -ge 8 ]; then
    ok "Python $PY_VER"
  else
    fail "Python $PY_VER (need 3.8+)"
  fi
else
  fail "Python 3 not found"
fi

# Required stdlib modules
for mod in csv json glob argparse subprocess webbrowser collections datetime re shutil; do
  python3 -c "import $mod" 2>/dev/null && ok "Module: $mod" || fail "Module: $mod"
done

# Chrome/Chromium (optional, for PDF)
CHROME_FOUND=0
for bin in "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
           "$(command -v google-chrome 2>/dev/null)" \
           "$(command -v chromium 2>/dev/null)" \
           "$(command -v chromium-browser 2>/dev/null)"; do
  if [ -n "$bin" ] && [ -x "$bin" ]; then
    ok "Chrome/Chromium found (PDF export available)"
    CHROME_FOUND=1
    break
  fi
done
[ "$CHROME_FOUND" -eq 0 ] && warn "Chrome/Chromium not found (PDF export will be skipped)"

# CDN connectivity
if curl -sI "https://d3js.org/d3.v7.min.js" | head -1 | grep -q "200"; then
  ok "D3.js CDN reachable"
else
  warn "D3.js CDN unreachable (charts need internet on first load)"
fi

echo ""
echo "  Ready to use: python3 generate_dashboard.py <service.csv> <device.csv>"
echo ""
