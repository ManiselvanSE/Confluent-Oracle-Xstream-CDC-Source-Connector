#!/usr/bin/env bash
# Create or update the XStream source connector and wait until it is RUNNING.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
log() { echo "[$(date +%H:%M:%S)] $*"; }

CONNECT=http://localhost:8083
# PUT .../config is create-or-update, so re-applying is safe
log "Applying connector $CONNECTOR_NAME"
curl -sf -X PUT -H 'Content-Type: application/json' \
  --data @connector/xstream-cdc.json "$CONNECT/connectors/$CONNECTOR_NAME/config" \
  | jq '{name, type, tasks: [.tasks[]?.task]}'

log "Waiting for connector + task to be RUNNING"
deadline=$((SECONDS + 300))
while :; do
  status=$(curl -s "$CONNECT/connectors/$CONNECTOR_NAME/status" || true)
  c=$(jq -r '.connector.state // "UNKNOWN"' <<<"$status")
  t=$(jq -r '.tasks[0].state // "NONE"' <<<"$status")
  log "  connector=$c task=$t"
  if [ "$c" = RUNNING ] && [ "$t" = RUNNING ]; then break; fi
  if [ "$c" = FAILED ] || [ "$t" = FAILED ]; then
    jq -r '.connector.trace // .tasks[0].trace' <<<"$status" | head -40
    exit 1
  fi
  [ $SECONDS -lt $deadline ] || { log "timeout"; docker logs --tail 80 connect; exit 1; }
  sleep 10
done
# Topics created before retention was configured keep their old setting,
# so align every CDC topic explicitly (idempotent).
log "Setting retention.ms=${TOPIC_RETENTION_MS} on ${TOPIC_PREFIX}.${CAPTURE_SCHEMA}.* topics"
for t in $(docker exec broker kafka-topics --bootstrap-server broker:29092 --list | grep "^${TOPIC_PREFIX}\.${CAPTURE_SCHEMA}\."); do
  docker exec broker kafka-configs --bootstrap-server broker:29092 --alter \
    --entity-type topics --entity-name "$t" --add-config "retention.ms=${TOPIC_RETENTION_MS}" >/dev/null
  log "  $t"
done
log "Connector stage complete"
