#!/usr/bin/env bash
# Start Oracle XE 21c and prepare it for XStream CDC. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
log() { echo "[$(date +%H:%M:%S)] $*"; }

log "Downloading ORDERMGMT sample schema scripts (reference repo, pinned commit)"
mkdir -p oracle/sample
for f in 02_create_user.sql 03_create_schema_datamodel.sql 04_load_data.sql 06_data_generator.sql; do
  [ -s "oracle/sample/$f" ] || curl -fsSL -o "oracle/sample/$f" "$SAMPLE_DATA_BASE_URL/$f"
done
# Scripts are mounted read-only into the container, oracle (uid 54321) must be able to read them
chmod -R a+rX oracle

if ! docker image inspect "$ORACLE_IMAGE" >/dev/null 2>&1; then
  log "Pulling $ORACLE_IMAGE (~3.5 GB compressed, several minutes)"
  docker pull -q "$ORACLE_IMAGE"
fi

log "Starting oracle21c container"
docker compose up -d oracle21c

# First start creates the database from scratch (10-25 min on 2 vCPUs).
# Progress is printed every 30s so the SSH session never looks idle.
log "Waiting for 'DATABASE IS READY TO USE!'"
deadline=$((SECONDS + 3600))
until docker logs oracle21c 2>&1 | grep -q "DATABASE IS READY TO USE"; do
  if docker logs oracle21c 2>&1 | grep -q "DATABASE SETUP WAS NOT SUCCESSFUL"; then
    docker logs --tail 80 oracle21c; exit 1
  fi
  [ $SECONDS -lt $deadline ] || { log "timeout"; docker logs --tail 80 oracle21c; exit 1; }
  log "  still starting... last log: $(docker logs --tail 1 oracle21c 2>&1 | cut -c1-120)"
  sleep 30
done
log "Database is open"

log "Preparing the database for XStream (runs once; marker in the oradata volume)"
docker exec oracle21c bash /opt/oracle/scripts/cdc/00_setup_cdc.sh

# The outbound SQL is re-applied on every run so capture parameters/connect user
# stay in sync with terraform.tfvars even after the one-time setup.
if docker exec oracle21c test -f /opt/oracle/oradata/.cdc_setup_done; then
  docker exec oracle21c bash -c \
    'sqlplus -s -L "${CDC_ADMIN_USER}/\"${CDC_ADMIN_PWD}\"@//localhost:1521/XE" @/opt/oracle/scripts/cdc/03_create_outbound.sql </dev/null'
fi
log "Oracle stage complete"
