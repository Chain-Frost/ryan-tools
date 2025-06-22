#!/usr/bin/env bash
# for chatgpt codex
set -e

# (Optional) Clone into /workspace if Codex didn't do that for you:
# git clone https://github.com/Chain-Frost/ryan-tools.git /workspace
# cd /workspace

# 1. Create your Python venv:
python3 -m venv .venv
source .venv/bin/activate

# 2. Install your dependencies:
pip install --upgrade pip
pip install -r requirements.txt

# 3. (If you use extra system tools or npm libs, install them here)
#    e.g. apt-get update && apt-get install -y libpq-dev
#          npm install -g typescript

deactivate
echo "📦  Setup complete!"
