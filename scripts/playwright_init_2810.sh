#!/bin/bash
# Cluster init script for the scraper job cluster (DBR 14.3 LTS, Ubuntu 22.04).
#
# Two modes
#   Online  (lab)   PLAYWRIGHT_ARTIFACTS_DIR not set: pip and Playwright download from the internet.
#   Offline (bank)  PLAYWRIGHT_ARTIFACTS_DIR set to a Volume folder prepared with
#                   scripts/build_playwright_artifacts.sh. Nothing is downloaded from the internet
#                   except OS packages, which must come from the bank approved apt mirror.
#
# Cluster environment variables (job cluster spark_env_vars)
#   PLAYWRIGHT_VERSION        pinned version, default below. Must match the approved artifact.
#   PLAYWRIGHT_BROWSERS_PATH  where Chromium lives. Must also be visible to notebooks.
#   PLAYWRIGHT_ARTIFACTS_DIR  optional, enables offline mode.
#
# Change from the original script: removed mesa-libgbm1 (a Fedora package name; apt rejects the
# whole install when one name is unknown). libgbm1 is the Ubuntu name and is in the list.

set -euo pipefail

PLAYWRIGHT_VERSION="${PLAYWRIGHT_VERSION:-1.47.0}"
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-/databricks/playwright-browsers}"
ARTIFACTS_DIR="${PLAYWRIGHT_ARTIFACTS_DIR:-}"
PIP=/databricks/python3/bin/pip
PY=/databricks/python3/bin/python

log() { echo "[playwright_init] $(date '+%Y%m%d %H:%M:%S') $*"; }

log "Installing OS libraries for headless Chromium"
apt-get update -q
apt-get install -y -q libgbm1 libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libasound2 libpango-1.0-0 libcairo2

mkdir -p "$PLAYWRIGHT_BROWSERS_PATH"

if [ -n "$ARTIFACTS_DIR" ]; then
    log "Offline mode, artifacts from $ARTIFACTS_DIR"
    test -d "$ARTIFACTS_DIR/wheels" || { log "Missing $ARTIFACTS_DIR/wheels"; exit 1; }
    test -f "$ARTIFACTS_DIR/playwright-browsers.tar.gz" || { log "Missing browser archive"; exit 1; }
    if [ -f "$ARTIFACTS_DIR/SHA256SUMS" ]; then
        (cd "$ARTIFACTS_DIR" && sha256sum --check --quiet SHA256SUMS) || { log "Artifact checksum failed"; exit 1; }
    fi
    $PIP install --no-index --find-links "$ARTIFACTS_DIR/wheels" "playwright==${PLAYWRIGHT_VERSION}"
    tar -xzf "$ARTIFACTS_DIR/playwright-browsers.tar.gz" -C "$PLAYWRIGHT_BROWSERS_PATH"
else
    log "Online mode, installing playwright ${PLAYWRIGHT_VERSION}"
    $PIP install "playwright==${PLAYWRIGHT_VERSION}"
    $PY -m playwright install chromium
    $PY -m playwright install-deps chromium
fi

log "Smoke test: launching headless Chromium"
$PY - <<'PYEOF'
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        print('Chromium', browser.version, 'OK')
        await browser.close()

asyncio.run(main())
PYEOF

log "Done"
