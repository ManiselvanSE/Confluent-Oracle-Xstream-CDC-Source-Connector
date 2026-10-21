#!/usr/bin/env bash
# End-to-end validation of the Oracle XE -> XStream -> Kafka pipeline.
# Exit code 0 = every check passed.
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

PASS=0; FAIL=0
ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
bad()  { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
check() { # check <description> <expected> <actual>
  if [ "$3" = "$2" ]; then ok "$1 = $3"; else bad "$1: expected '$2', got '$3'"; fi
}

# sql <connect> <query> -> single trimmed value
sql() {
  docker exec -i -e ORACLE_SID=XE oracle21c sqlplus -s -L "$1" <<EOF | tr -d '[:space:]'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF
$2
EXIT
EOF
}
SYS='/ as sysdba'

echo "== 1. Containers"
for c in oracle21c broker schema-registry connect; do
  check "container $c" running "$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo missing)"
done

echo "== 2. Oracle database prerequisites"
check "log_mode"                      ARCHIVELOG "$(sql "$SYS" "SELECT log_mode FROM v\$database;")"
check "supplemental logging (min)"    YES        "$(sql "$SYS" "SELECT supplemental_log_data_min FROM v\$database;")"
check "supplemental logging (all)"    YES        "$(sql "$SYS" "SELECT supplemental_log_data_all FROM v\$database;")"
check "enable_goldengate_replication" TRUE       "$(sql "$SYS" "SELECT UPPER(value) FROM v\$parameter WHERE name='enable_goldengate_replication';")"
check "PDB $ORACLE_PDB open mode"     READWRITE  "$(sql "$SYS" "SELECT open_mode FROM v\$pdbs WHERE name='$ORACLE_PDB';")"

echo "== 3. XStream objects"
check "outbound server"            "$OUTBOUND_SERVER" "$(sql "$SYS" "SELECT server_name FROM dba_xstream_outbound WHERE server_name='$OUTBOUND_SERVER';")"
check "outbound connect_user"      "$CDC_USER"        "$(sql "$SYS" "SELECT connect_user FROM dba_xstream_outbound WHERE server_name='$OUTBOUND_SERVER';")"
check "capture $CAPTURE_NAME status" ENABLED          "$(sql "$SYS" "SELECT status FROM dba_capture WHERE capture_name='$CAPTURE_NAME';")"
check "outbound (apply) status"    ENABLED            "$(sql "$SYS" "SELECT status FROM dba_apply WHERE apply_name='$OUTBOUND_SERVER';")"
attached=$(sql "$SYS" "SELECT COUNT(*) FROM gv\$xstream_outbound_server WHERE server_name='$OUTBOUND_SERVER';")
if [ "${attached:-0}" -ge 1 ]; then ok "connector attached to $OUTBOUND_SERVER"; else bad "no client attached to $OUTBOUND_SERVER"; fi

echo "== 4. Kafka / Schema Registry / Connect"
if docker exec broker kafka-topics --bootstrap-server broker:29092 --list >/dev/null 2>&1; then ok "broker reachable"; else bad "broker not reachable"; fi
if curl -sf http://localhost:8081/subjects >/dev/null; then ok "schema registry reachable"; else bad "schema registry not reachable"; fi
if curl -sf http://localhost:8083/connector-plugins | grep -q OracleXStreamSourceConnector; then ok "XStream plugin installed"; else bad "XStream plugin missing"; fi
status=$(curl -s "http://localhost:8083/connectors/$CONNECTOR_NAME/status")
check "connector $CONNECTOR_NAME" RUNNING "$(jq -r '.connector.state // "MISSING"' <<<"$status")"
check "task 0"                    RUNNING "$(jq -r '.tasks[0].state // "MISSING"' <<<"$status")"
TOPICS=$(docker exec broker kafka-topics --bootstrap-server broker:29092 --list 2>/dev/null | grep "^${TOPIC_PREFIX}\.${CAPTURE_SCHEMA}\." || true)
n_topics=$(grep -c . <<<"$TOPICS" || true)
if [ "$n_topics" -ge 1 ]; then ok "$n_topics CDC topics (${TOPIC_PREFIX}.${CAPTURE_SCHEMA}.*)"; else bad "no CDC topics found"; fi
wrong=""
for t in $TOPICS; do
  r=$(docker exec broker kafka-configs --bootstrap-server broker:29092 --describe --entity-type topics --entity-name "$t" \
        | grep -oE '^  retention.ms=[^ ]+' | cut -d= -f2)
  [ "$r" = "$TOPIC_RETENTION_MS" ] || wrong+=" $t=${r:-default}"
done
if [ -z "$wrong" ]; then ok "retention.ms=${TOPIC_RETENTION_MS} on all CDC topics"; else bad "retention.ms differs:$wrong"; fi

if [ "$C3_ENABLED" = true ]; then
  echo "== 5. Control Center"
  for c in prometheus alertmanager control-center; do
    check "container $c" running "$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo missing)"
  done
  if curl -sf http://localhost:9090/-/ready >/dev/null; then ok "prometheus ready"; else bad "prometheus not ready"; fi
  if curl -sf -o /dev/null http://localhost:9021/; then ok "control center UI reachable (:9021)"; else bad "control center UI not reachable"; fi
fi

# count_rows <schema> -> "TABLE=count ..." for every captured table
count_rows() {
  local q="" t
  for t in $(sed 's/.*\.//' <<<"$TOPICS"); do q+="SELECT '${t}='||COUNT(*) FROM $1.\"${t}\";"$'\n'; done
  docker exec -i -e ORACLE_SID=XE oracle21c sqlplus -s -L / as sysdba <<EOF | grep '=' | sort | tr '\n' ' '
SET HEADING OFF FEEDBACK OFF PAGESIZE 0
ALTER SESSION SET CONTAINER = ${ORACLE_PDB};
${q}EXIT
EOF
}

if [ "$JDBC_SINK_ENABLED" = true ]; then
  echo "== 6. JDBC sink -> ${ORACLE_PDB}.${SINK_SCHEMA}"
  status=$(curl -s "http://localhost:8083/connectors/$JDBC_SINK_NAME/status")
  check "connector $JDBC_SINK_NAME" RUNNING "$(jq -r '.connector.state // "MISSING"' <<<"$status")"
  check "task 0"                    RUNNING "$(jq -r '.tasks[0].state // "MISSING"' <<<"$status")"
  # The sink may still be catching up right after deployment
  for attempt in $(seq 1 12); do
    src=$(count_rows "$CAPTURE_SCHEMA"); dst=$(count_rows "$SINK_SCHEMA")
    [ "$src" = "$dst" ] && break
    sleep 10
  done
  if [ "$src" = "$dst" ]; then
    ok "row counts match for $(wc -w <<<"$src") tables"
  else
    bad "row counts differ"; echo "        source: $src"; echo "        sink:   $dst"
  fi
fi

echo "== 7. End-to-end: INSERT/DELETE in Oracle -> event in Kafka${JDBC_SINK_ENABLED/true/ -> ${SINK_SCHEMA}}"
MARKER="VALIDATE_$(date +%s)"
TOPIC="${TOPIC_PREFIX}.${CAPTURE_SCHEMA}.REGIONS"
docker exec -i oracle21c sqlplus -s -L "ordermgmt/kafka@//localhost:1521/$ORACLE_PDB" >/dev/null <<EOF
INSERT INTO regions (region_name) VALUES ('$MARKER');
COMMIT;
EXIT
EOF
echo "  inserted REGIONS.REGION_NAME='$MARKER', consuming $TOPIC ..."
found=no
for attempt in 1 2 3 4; do
  out=$(docker exec schema-registry kafka-avro-console-consumer \
          --bootstrap-server broker:29092 --topic "$TOPIC" --from-beginning \
          --property schema.registry.url=http://schema-registry:8081 \
          --timeout-ms 20000 2>/dev/null || true)
  if grep -q "$MARKER" <<<"$out"; then found=yes; break; fi
  echo "  not yet (attempt $attempt)"
done
if [ "$found" = yes ]; then
  ok "change event received in $TOPIC"
  grep "$MARKER" <<<"$out" | tail -1 | cut -c1-300 | sed 's/^/        /'
else
  bad "change event for $MARKER not found in $TOPIC"
fi
# wait_sink_rows <expected> -> rows with the marker in the sink's REGIONS table
wait_sink_rows() {
  local n attempt
  for attempt in $(seq 1 12); do
    n=$(docker exec -i -e ORACLE_SID=XE oracle21c sqlplus -s -L / as sysdba <<EOF | tr -d '[:space:]'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0
ALTER SESSION SET CONTAINER = ${ORACLE_PDB};
SELECT COUNT(*) FROM ${SINK_SCHEMA}."REGIONS" WHERE "REGION_NAME" = '$MARKER';
EXIT
EOF
)
    [ "$n" = "$1" ] && break
    sleep 5
  done
  echo "$n"
}

if [ "$JDBC_SINK_ENABLED" = true ]; then
  check "insert replicated to ${SINK_SCHEMA}.REGIONS (rows)" 1 "$(wait_sink_rows 1)"
fi

docker exec -i oracle21c sqlplus -s -L "ordermgmt/kafka@//localhost:1521/$ORACLE_PDB" >/dev/null <<EOF
DELETE FROM regions WHERE region_name = '$MARKER';
COMMIT;
EXIT
EOF

if [ "$JDBC_SINK_ENABLED" = true ]; then
  check "delete replicated to ${SINK_SCHEMA}.REGIONS (rows)" 0 "$(wait_sink_rows 0)"
fi

echo "== Result: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
