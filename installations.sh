#set -e

cd "$(dirname "$0")"

echo "Changed dir to: $(pwd)"
echo "Updating package list..."
sudo apt-get update

echo "Installing system dependencies..."
sudo apt-get install -y \
python3-pip \
python3-dev \
default-libmysqlclient-dev \
build-essential \
pkg-config

echo "Installing python system dependencies..."
pip install uv

echo "Generating .venv and activating..."
uv venv
source .venv/bin/activate

echo "Installing requirements..."
uv pip install -r requirements.txt
uv pip install mysqlclient==2.2.7

echo "Copying services..."
