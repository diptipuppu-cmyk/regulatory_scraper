#!/bin/bash
# Update OS package list
apt-get update

# Install required Linux libraries for Chromium headless browser
apt-get install -y libgbm1 libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 mesa-libgbm1

# Install Playwright Python package
/databricks/python3/bin/pip install playwright

# Download Chromium browser binary and its specific dependencies
/databricks/python3/bin/playwright install chromium
/databricks/python3/bin/playwright install-deps chromium