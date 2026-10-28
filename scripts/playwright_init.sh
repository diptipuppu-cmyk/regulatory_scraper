#!/bin/bash
# Cluster init script for the scraper job cluster (Ubuntu 22.04 based Databricks Runtime).
# Installs the Playwright driver and Microsoft Edge Stable (the latest version at every cluster start).
#
# Two modes
#   Online  (lab)   PLAYWRIGHT_ARTIFACTS_DIR not set: Playwright from PyPI, Edge from the Microsoft repository.
#   Offline (bank)  PLAYWRIGHT_ARTIFACTS_DIR set to a Volume folder with the reviewed Playwright wheels.
#                   Edge from the bank approved apt mirror (package microsoft-edge-stable).
#
# Cluster environment variables (job cluster spark_env_vars)
#   PLAYWRIGHT_VERSION        pinned Playwright version, default below.
#   PLAYWRIGHT_ARTIFACTS_DIR  optional, enables offline mode.

set -euo pipefail

PLAYWRIGHT_VERSION="${PLAYWRIGHT_VERSION:-1.47.0}"
ARTIFACTS_DIR="${PLAYWRIGHT_ARTIFACTS_DIR:-}"
PIP=/databricks/python3/bin/pip
PY=/databricks/python3/bin/python
EDGE=/usr/bin/microsoft-edge-stable

log() { echo "[playwright_init] $(date '+%Y%m%d %H:%M:%S') $*"; }

log "Installing OS libraries for headless Microsoft Edge"
apt-get update -q
apt-get install -y -q libgbm1 libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libasound2 libpango-1.0-0 libcairo2

if [ -n "$ARTIFACTS_DIR" ]; then
    log "Offline mode, Playwright wheels from $ARTIFACTS_DIR"
    test -d "$ARTIFACTS_DIR/wheels" || { log "Missing $ARTIFACTS_DIR/wheels"; exit 1; }
    if [ -f "$ARTIFACTS_DIR/SHA256SUMS" ]; then
        (cd "$ARTIFACTS_DIR" && sha256sum --check --quiet SHA256SUMS) || { log "Artifact checksum failed"; exit 1; }
    fi
    $PIP install --no-index --find-links "$ARTIFACTS_DIR/wheels" "playwright==${PLAYWRIGHT_VERSION}"
    log "Installing Microsoft Edge Stable from the approved apt mirror"
    apt-get install -y -q microsoft-edge-stable
else
    log "Online mode, installing playwright ${PLAYWRIGHT_VERSION}"
    $PIP install "playwright==${PLAYWRIGHT_VERSION}"
    log "Installing Microsoft Edge Stable from the Microsoft repository"
    $PY -m playwright install msedge
fi

test -x "$EDGE" || { log "Microsoft Edge was not installed"; exit 1; }
log "Installed: $($EDGE --version)"

log "Smoke test: launching headless Microsoft Edge"
$PY - <<'PYEOF'
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, channel='msedge')
        print('Microsoft Edge', browser.version, 'OK')
        await browser.close()

asyncio.run(main())
PYEOF

log "Done"
