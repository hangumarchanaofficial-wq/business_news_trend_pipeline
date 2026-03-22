#!/bin/bash
# ══════════════════════════════════════════════════════════════
#  EC2 Setup Script — Ubuntu 22.04 / Amazon Linux 2023
#  Run once after launching the instance:
#    chmod +x setup_ec2.sh && ./setup_ec2.sh
# ══════════════════════════════════════════════════════════════

set -e

echo "════════════════════════════════════════════════"
echo "  Setting up Business News Trend Pipeline"
echo "════════════════════════════════════════════════"

# ── 1. System packages ────────────────────────────────────────
sudo apt-get update -y
sudo apt-get install -y \
    python3.11 python3.11-venv python3.11-dev python3-pip \
    openjdk-17-jdk-headless \
    git curl unzip wget \
    libnss3 libatk-bridge2.0-0 libdrm2 libxkbcommon0 \
    libgbm1 libgtk-3-0 libasound2 libxshmfence1

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc

echo "Java version:"
java -version

# ── 2. Python virtual environment ────────────────────────────
python3.11 -m venv venv
source venv/bin/activate

echo "Python version:"
python --version

# ── 3. Python dependencies ───────────────────────────────────
pip install --upgrade pip
pip install -r requirements.txt

# ── 4. Playwright browsers ───────────────────────────────────
playwright install chromium
playwright install-deps chromium

# ── 5. NLTK data ─────────────────────────────────────────────
python -c "
import nltk
nltk.download('stopwords', quiet=True)
nltk.download('punkt_tab', quiet=True)
print('NLTK data downloaded')
"

# ── 6. Create data directories ───────────────────────────────
mkdir -p data/raw data/warehouse logs

# ── 7. AWS CLI (if not pre-installed) ────────────────────────
if ! command -v aws &> /dev/null; then
    echo "Installing AWS CLI..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
        -o "awscliv2.zip"
    unzip -q awscliv2.zip
    sudo ./aws/install
    rm -rf aws awscliv2.zip
fi

echo ""
echo "════════════════════════════════════════════════"
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "    1. Configure AWS:  aws configure"
echo "    2. Edit bucket:    nano config/aws_config.yaml"
echo "    3. Run pipeline:   source venv/bin/activate"
echo "                       python main.py"
echo "════════════════════════════════════════════════"