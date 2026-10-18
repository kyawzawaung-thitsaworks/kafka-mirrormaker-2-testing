#!/usr/bin/env bash
# One-time setup on kafka-backup: Python environment and libraries.
set -euo pipefail
cd "$(dirname "$0")"

echo "Installing python3-venv (needs sudo)…"
sudo apt-get update -qq
sudo apt-get install -y -qq python3-venv

echo "Creating the Python environment in .venv…"
python3 -m venv .venv
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r requirements.txt

chmod +x run.sh slow-link.sh lag-test.sh
echo
echo "Setup done. Next: ./run.sh start"
