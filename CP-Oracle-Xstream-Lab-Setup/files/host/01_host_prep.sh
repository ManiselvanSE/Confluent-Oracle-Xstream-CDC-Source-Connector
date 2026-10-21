#!/usr/bin/env bash
# Host preparation: Docker engine, Compose + Buildx plugins, OS limits, swap.
# Idempotent - safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
log() { echo "[$(date +%H:%M:%S)] $*"; }

log "Installing packages (docker, jq, unzip)"
dnf -y -q install docker jq unzip tar

PLUGINS=/usr/local/lib/docker/cli-plugins
mkdir -p "$PLUGINS"
if ! docker compose version 2>/dev/null | grep -q "${DOCKER_COMPOSE_VERSION#v}"; then
  log "Installing Docker Compose ${DOCKER_COMPOSE_VERSION}"
  curl -fsSL -o "$PLUGINS/docker-compose" \
    "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64"
  chmod +x "$PLUGINS/docker-compose"
fi
if ! docker buildx version 2>/dev/null | grep -q "${DOCKER_BUILDX_VERSION}"; then
  log "Installing Docker Buildx ${DOCKER_BUILDX_VERSION}"
  curl -fsSL -o "$PLUGINS/docker-buildx" \
    "https://github.com/docker/buildx/releases/download/${DOCKER_BUILDX_VERSION}/buildx-${DOCKER_BUILDX_VERSION}.linux-amd64"
  chmod +x "$PLUGINS/docker-buildx"
fi

# Rotate container logs so they cannot fill the disk. Only written once:
# changing it later needs a docker restart, which would bounce the stack.
if [ ! -f /etc/docker/daemon.json ]; then
  mkdir -p /etc/docker
  cat > /etc/docker/daemon.json <<'EOF'
{
  "log-driver": "json-file",
  "log-opts": { "max-size": "50m", "max-file": "3" }
}
EOF
fi

# Kafka and Oracle both want many file handles and memory maps
cat > /etc/sysctl.d/99-cdc-xstream.conf <<'EOF'
vm.max_map_count = 262144
fs.file-max = 6815744
EOF
sysctl -q --system
cat > /etc/security/limits.d/99-cdc-xstream.conf <<'EOF'
*  soft  nofile  65535
*  hard  nofile  65535
EOF

systemctl enable --now docker
usermod -aG docker ec2-user

if [ "${SWAP_SIZE_GB}" -gt 0 ] && ! swapon --show | grep -q /swapfile; then
  log "Creating ${SWAP_SIZE_GB} GB swap file"
  fallocate -l "${SWAP_SIZE_GB}G" /swapfile
  chmod 600 /swapfile
  mkswap /swapfile >/dev/null
  swapon /swapfile
  grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

log "Docker: $(docker --version)"
log "Compose: $(docker compose version --short)"
log "Buildx: $(docker buildx version | awk '{print $2}')"
log "Host preparation complete"
