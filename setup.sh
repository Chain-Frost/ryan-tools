#!/usr/bin/env bash
# for chatgpt codex
set -e

# (Optional) Clone into /workspace if Codex didn't do that for you:
# git clone https://github.com/Chain-Frost/ryan-tools.git /workspace
# cd /workspace

# Install into the user's Python library. This repository does not use a
# repository-local virtual environment.
python3 -m pip install --user --upgrade pip
python3 -m pip install --user -r requirements.txt

# If you use extra system tools or npm libs, install them here.
#    e.g. apt-get update && apt-get install -y libpq-dev
#          npm install -g typescript

echo "📦  Setup complete!"
