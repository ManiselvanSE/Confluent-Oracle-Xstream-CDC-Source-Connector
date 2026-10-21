#!/usr/bin/env bash
# Create the Oracle sink schema and the JDBC sink connector that replays the
# CDC topics into it, then wait until it is RUNNING. Idempotent.
# With JDBC_SINK_ENABLED=false the connector is deleted (schema/tables kept).
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
log() { echo "[$(date +%H:%M:%S)] $*"; }

CONNECT=http://localhost:8083

if [ "$JDBC_SINK_ENABLED" != true ]; then
  if curl -sf "$CONNECT/connectors/$JDBC_SINK_NAME" >/dev/null; then
    log "JDBC sink disabled - deleting connector $JDBC_SINK_NAME (schema $SINK_SCHEMA is kept)"
    curl -sf -X DELETE "$CONNECT/connectors/$JDBC_SINK_NAME"
  else
    log "JDBC sink disabled - nothing to do"
  fi
  exit 0
fi

# Password goes through stdin, never on a command line
log "Creating (or resetting password of) sink schema $SINK_SCHEMA in $ORACLE_PDB"
docker exec -i -e ORACLE_SID=XE oracle21c sqlplus -s -L / as sysdba <<EOF
WHENEVER SQLERROR EXIT SQL.SQLCODE
SET FEEDBACK OFF SERVEROUTPUT ON
ALTER SESSION SET CONTAINER = ${ORACLE_PDB};
DECLARE
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n FROM dba_users WHERE username = '${SINK_SCHEMA}';
  IF n = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER ${SINK_SCHEMA} IDENTIFIED BY "${SINK_SCHEMA_PWD}" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users';
    DBMS_OUTPUT.PUT_LINE('created user ${SINK_SCHEMA}');
  ELSE
    EXECUTE IMMEDIATE 'ALTER USER ${SINK_SCHEMA} IDENTIFIED BY "${SINK_SCHEMA_PWD}" ACCOUNT UNLOCK';
    DBMS_OUTPUT.PUT_LINE('user ${SINK_SCHEMA} exists - password reset');
  END IF;
END;
/
GRANT CREATE SESSION, CREATE TABLE TO ${SINK_SCHEMA};
EXIT
EOF

# PUT .../config is create-or-update, so re-applying is safe
log "Applying connector $JDBC_SINK_NAME"
curl -sf -X PUT -H 'Content-Type: application/json' \
  --data @connector/jdbc-sink.json "$CONNECT/connectors/$JDBC_SINK_NAME/config" \
  | jq '{name, type, topics: .config.topics}'

log "Waiting for connector + task to be RUNNING"
deadline=$((SECONDS + 300))
while :; do
  status=$(curl -s "$CONNECT/connectors/$JDBC_SINK_NAME/status" || true)
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

# A task can be RUNNING and still fail on its first batch (e.g. a DDL
# error), so give it one poll interval and re-check.
sleep 20
t=$(curl -s "$CONNECT/connectors/$JDBC_SINK_NAME/status" | jq -r '.tasks[0].state')
if [ "$t" != RUNNING ]; then
  curl -s "$CONNECT/connectors/$JDBC_SINK_NAME/status" | jq -r '.tasks[0].trace' | head -40
  exit 1
fi
log "JDBC sink stage complete"
