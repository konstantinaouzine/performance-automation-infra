#!/usr/bin/env bash
set -euo pipefail

# Usage: ./register_runner.sh <github_repo_url> <token> [labels]
# Example: ./register_runner.sh https://github.com/konstantinaouzine/performance-automation-infra ABC123 self-hosted,ansible,jmeter

REPO_URL=${1:-}
TOKEN=${2:-}
LABELS=${3:-self-hosted}

if [[ -z "$REPO_URL" || -z "$TOKEN" ]]; then
  echo "Usage: $0 <github_repo_url> <token> [labels]" >&2
  exit 1
fi

RUNNER_DIR=${RUNNER_DIR:-$HOME/actions-runner}
mkdir -p "$RUNNER_DIR"
cd "$RUNNER_DIR"

if [[ ! -f .runner ]]; then
  echo "Downloading latest runner release metadata..."
  API_JSON=$(curl -s https://api.github.com/repos/actions/runner/releases/latest)
  TAR_URL=$(echo "$API_JSON" | grep browser_download_url | grep linux-x64 | cut -d '"' -f4 | head -n1)
  [[ -z "$TAR_URL" ]] && { echo "Could not determine runner tarball URL" >&2; exit 1; }
  echo "Downloading $TAR_URL"
  curl -sL "$TAR_URL" -o actions-runner.tar.gz
  tar -xzf actions-runner.tar.gz
fi

# Install dependencies (Ubuntu)
if command -v apt-get >/dev/null; then
  sudo apt-get update -y
  sudo apt-get install -y libicu-dev libssl-dev curl jq
fi

# Remove previous config if partially configured
if [[ -f .runner && ! -f .service ]]; then
  echo "Runner metadata exists. If you need a clean re-config, run: ./config.sh remove --token <token>"
fi

./config.sh --url "$REPO_URL" --token "$TOKEN" --labels "$LABELS" --unattended

if [[ $EUID -eq 0 ]]; then
  echo "Installing as root service..."
  ./svc.sh install
  ./svc.sh start
else
  echo "Installing user-level service (systemd) if supported..."
  if [[ -f ./svc.sh ]]; then
    sudo ./svc.sh install
    sudo ./svc.sh start
  else
    echo "Run manually: ./run.sh"
  fi
fi

echo "Runner registered. Status should be Idle in repo settings."
