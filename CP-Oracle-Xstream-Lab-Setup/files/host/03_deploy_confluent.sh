#!/usr/bin/env bash
# Build the Connect image (XStream + JDBC connectors, Instant Client) and
# start the Confluent Platform 8.x services, plus Control Center when the
# "c3" compose profile is enabled. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
log() { echo "[$(date +%H:%M:%S)] $*"; }

PLUGIN="confluentinc-kafka-connect-oracle-xstream-cdc-source-${XSTREAM_CONNECTOR_VERSION}"
STAGE=connect/stage
mkdir -p "$STAGE"

if [ ! -f "$STAGE/instantclient/xstreams.jar" ] || [ "$(cat "$STAGE/instantclient.url" 2>/dev/null)" != "$INSTANT_CLIENT_URL" ]; then
  log "Downloading Oracle Instant Client"
  rm -rf "$STAGE/instantclient" "$STAGE"/instantclient_*
  curl -fsSL -o "$STAGE/ic.zip" "$INSTANT_CLIENT_URL"
  unzip -q -o "$STAGE/ic.zip" -d "$STAGE"
  mv "$STAGE"/instantclient_* "$STAGE/instantclient"
  rm -f "$STAGE/ic.zip"
  echo "$INSTANT_CLIENT_URL" > "$STAGE/instantclient.url"
fi

if [ ! -d "$STAGE/$PLUGIN" ]; then
  log "Downloading XStream CDC connector ${XSTREAM_CONNECTOR_VERSION} from Confluent Hub"
  curl -fsSL -o "$STAGE/plugin.zip" \
    "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-oracle-xstream-cdc-source/versions/${XSTREAM_CONNECTOR_VERSION}/${PLUGIN}.zip"
  unzip -q -o "$STAGE/plugin.zip" -d "$STAGE"
  rm -f "$STAGE/plugin.zip"
fi

JDBC_PLUGIN="confluentinc-kafka-connect-jdbc-${JDBC_CONNECTOR_VERSION}"
if [ ! -d "$STAGE/$JDBC_PLUGIN" ]; then
  log "Downloading JDBC connector ${JDBC_CONNECTOR_VERSION} from Confluent Hub"
  curl -fsSL -o "$STAGE/jdbc.zip" \
    "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/versions/${JDBC_CONNECTOR_VERSION}/${JDBC_PLUGIN}.zip"
  unzip -q -o "$STAGE/jdbc.zip" -d "$STAGE"
  rm -f "$STAGE/jdbc.zip"
fi

# Connector requirement: ojdbc8.jar + xstreams.jar from the same Instant Client in the plugin lib/
cp "$STAGE/instantclient/ojdbc8.jar" "$STAGE/instantclient/xstreams.jar" "$STAGE/$PLUGIN/lib/"

C3_SERVICES=""
if [ "$C3_ENABLED" = true ]; then
  C3_SERVICES="prometheus alertmanager control-center"
  # Control Center rewrites the *-generated.yml files; the Prometheus and
  # Alertmanager images run as non-root users, so the dir must be writable.
  chmod -R a+rwX c3
else
  docker compose --profile c3 rm -sf prometheus alertmanager control-center >/dev/null 2>&1 || true
fi

log "Pulling Confluent Platform ${CP_VERSION} images ${C3_SERVICES:+(+ Control Center ${C3_VERSION})}"
docker compose pull -q broker schema-registry $C3_SERVICES

log "Building Connect image"
docker compose build connect

log "Starting broker, schema-registry, connect $C3_SERVICES"
docker compose up -d broker schema-registry connect $C3_SERVICES

wait_for() { # wait_for <name> <timeout-seconds> <command...>
  local name=$1 timeout=$2; shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    [ $SECONDS -lt $deadline ] || { log "timeout waiting for $name"; docker logs --tail 60 "$name"; exit 1; }
    log "  waiting for $name..."
    sleep 10
  done
  log "$name is ready"
}

wait_for broker 300 docker exec broker kafka-topics --bootstrap-server broker:29092 --list
wait_for schema-registry 300 curl -sf http://localhost:8081/subjects
wait_for connect 600 curl -sf http://localhost:8083/connectors
if [ "$C3_ENABLED" = true ]; then
  wait_for prometheus 300 curl -sf http://localhost:9090/-/ready
  wait_for control-center 600 curl -sf http://localhost:9021/
fi

if ! curl -sf http://localhost:8083/connector-plugins | grep -q OracleXStreamSourceConnector; then
  log "XStream connector plugin NOT loaded"; docker logs --tail 80 connect; exit 1
fi
if ! curl -sf http://localhost:8083/connector-plugins | grep -q JdbcSinkConnector; then
  log "JDBC connector plugin NOT loaded"; docker logs --tail 80 connect; exit 1
fi
log "Plugins loaded:"
curl -s http://localhost:8083/connector-plugins | jq -r '.[] | select(.class | test("XStream|Jdbc")) | "  \(.class) \(.version)"'
log "Confluent stage complete"
