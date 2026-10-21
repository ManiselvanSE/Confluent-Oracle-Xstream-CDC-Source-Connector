#!/usr/bin/env bash
# Presenter helper for the customer demo - see DEMO.md for the full script.
# Run on the host:  ~/cdc-xstream/bin/demo.sh <command> [args]
# Every command prints the SQL / REST call it runs, so the audience sees it.
set -uo pipefail
SELF=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

CONNECT=http://localhost:8083
SR=http://localhost:8081
BASE="${TOPIC_PREFIX}.${CAPTURE_SCHEMA}"
STATE=.demo_baseline          # MAX(order_id) before the demo; reset deletes above it
DOCKER=docker; docker ps >/dev/null 2>&1 || DOCKER="sudo docker"

B=$'\e[1m'; C=$'\e[36m'; G=$'\e[32m'; R=$'\e[31m'; Y=$'\e[33m'; N=$'\e[0m'
title() { echo; echo "${B}${C}== $*${N}"; }
show()  { echo "${Y}\$ $*${N}"; }                    # echo a command before running it
showsql() { sed "s/^/${Y}SQL> /;s/\$/${N}/" <<<"$1"; }

# src_sql <sql>  - as the application user ORDERMGMT in the PDB
src_sql() {
  $DOCKER exec -i oracle21c sqlplus -s -L "ordermgmt/kafka@//localhost:1521/$ORACLE_PDB" <<EOF
SET LINESIZE 250 PAGESIZE 100 FEEDBACK ON TRIMOUT ON SERVEROUTPUT ON
WHENEVER SQLERROR EXIT SQL.SQLCODE
$1
EXIT
EOF
}
# pdb_sql <sql>  - as SYSDBA inside the PDB (can read both schemas)
pdb_sql() {
  $DOCKER exec -i -e ORACLE_SID=XE oracle21c sqlplus -s -L / as sysdba <<EOF
SET LINESIZE 250 PAGESIZE 100 FEEDBACK OFF TRIMOUT ON NUMWIDTH 20
ALTER SESSION SET CONTAINER = ${ORACLE_PDB};
SET SERVEROUTPUT ON
$1
EXIT
EOF
}
# table view: '|' separated -> aligned columns
grid() { grep -v '^$' | column -t -s'|'; }
pdb_grid() { pdb_sql "SET MARKUP CSV ON DELIMITER | QUOTE OFF
$1" | grep -v -e '^Session altered' | grid; }
val() { pdb_sql "SET HEADING OFF PAGESIZE 0
$1" | grep -v '^Session altered' | tr -d '[:space:]'; }

baseline() {
  [ -s "$STATE" ] || val "SELECT MAX(order_id) FROM ${CAPTURE_SCHEMA}.orders;" >"$STATE"
  cat "$STATE"
}
last_order() { val "SELECT NVL(MAX(order_id),0) FROM ${CAPTURE_SCHEMA}.orders WHERE order_id > $(baseline);"; }
tbl() { echo "${1:-ORDERS}" | tr '[:lower:]' '[:upper:]'; }

# Avro JSON -> plain JSON: drop union wrappers like {"int":54} and {"XEPDB1....Value":{...}}
JQ_UNWRAP='def u: walk(if type=="object" and length==1 and (keys[0] | test("^(null|int|long|float|double|boolean|string|bytes)$") or test("[.]")) then .[] else . end);'
# Avro decimals are printed as raw bytes (one char per byte, big-endian two's
# complement); decode them with the field's scale from Schema Registry.
JQ_DEC='def dec($s): explode as $b | (reduce $b[] as $x (0; . * 256 + $x)) as $n
  | (if ($b | length) > 0 and $b[0] >= 128 then $n - pow(256; $b | length) else $n end) / pow(10; $s);
def fix($m): if type == "object" and $m != null
  then with_entries(.key as $k | if $m[$k] != null and (.value | type) == "string" then .value |= dec($m[$k]) else . end)
  else . end;'
# scale_map TABLE... -> {"T": {field: scale}, "T-key": {...}, "_keys": {...all key fields}}
scale_map() {
  local t
  for t in "$@"; do
    curl -s "$SR/subjects/${BASE}.${t}-value/versions/latest/schema" | jq --arg t "$t" '
      def decs: [.[] | {name, t: (.type | if type == "array" then (map(objects) | .[0]) else . end)}
                 | select(.t | type == "object" and .logicalType == "decimal") | {(.name): .t.scale}] | add // {};
      {($t): ([.fields[] | select(.name == "before") | .type[] | objects | .fields[]] | decs)}'
    curl -s "$SR/subjects/${BASE}.${t}-key/versions/latest/schema" | jq --arg t "$t" '
      {("\($t)-key"): ([.fields[] | {name, t: (.type | if type == "array" then (map(objects) | .[0]) else . end)}
                         | select(.t | type == "object" and .logicalType == "decimal") | {(.name): .t.scale}] | add // {})}'
  done | jq -s 'add | . + {_keys: ([to_entries[] | select(.key | endswith("-key")) | .value] | add // {})}'
}

connector_states() {
  curl -s "$CONNECT/connectors?expand=status" |
    jq -r 'to_entries[] | "\(.key)\t\(.value.status.type)\tconnector=\(.value.status.connector.state)\ttask=\(.value.status.tasks[0].state // "NONE")"' | column -t
}
wait_running() { # wait until every connector + task is RUNNING
  local i
  for i in $(seq 1 60); do
    s=$(curl -s "$CONNECT/connectors?expand=status" 2>/dev/null) || s=""
    jq -e 'type == "object"' >/dev/null 2>&1 <<<"$s" || s=""   # REST API still starting
    if [ -n "$s" ] && [ "$(jq '[.[].status | .connector.state, .tasks[].state] | all(. == "RUNNING")' <<<"$s")" = true ] \
       && [ "$(jq 'length' <<<"$s")" -ge 1 ]; then
      connector_states; return 0
    fi
    printf '.'; sleep 5
  done
  echo; echo "${R}connectors not RUNNING after 5 min${N}"; connector_states; return 1
}

cmd=${1:-help}; shift || true
case "$cmd" in

status)
  title "Containers (docker compose project cdc-xstream)"
  show "docker ps"
  $DOCKER ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
  title "Connectors"
  show "curl $CONNECT/connectors?expand=status"
  connector_states
  ;;

oracle)
  title "Oracle prerequisites for XStream CDC"
  pdb_grid "ALTER SESSION SET CONTAINER = CDB\$ROOT;
SELECT name, log_mode, supplemental_log_data_min AS supp_min, supplemental_log_data_all AS supp_all, force_logging FROM v\$database;
SELECT name, value FROM v\$parameter WHERE name IN ('enable_goldengate_replication','streams_pool_size','db_recovery_file_dest_size');"
  title "XStream capture + outbound server"
  pdb_grid "ALTER SESSION SET CONTAINER = CDB\$ROOT;
SELECT capture_name, status, capture_user, TO_CHAR(captured_scn) captured_scn FROM dba_capture;
SELECT server_name, connect_user, capture_name, source_database, status FROM dba_xstream_outbound;
SELECT server_name, state, total_messages_sent FROM gv\$xstream_outbound_server;"
  title "Captured tables (${CAPTURE_SCHEMA}) and their primary keys = Kafka message keys"
  in=$($DOCKER exec broker kafka-topics --bootstrap-server broker:29092 --list | grep "^${BASE}\." | sed "s/.*\./'/; s/\$/'/" | paste -sd,)
  pdb_grid "SELECT c.table_name, LISTAGG(cc.column_name, ', ') WITHIN GROUP (ORDER BY cc.position) AS primary_key
FROM dba_constraints c JOIN dba_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
WHERE c.owner = '${CAPTURE_SCHEMA}' AND c.constraint_type = 'P' AND c.table_name IN (${in})
GROUP BY c.table_name ORDER BY 1;"
  ;;

plugins)
  title "Connector plugins installed in the Connect worker"
  show "curl $CONNECT/connector-plugins"
  curl -s "$CONNECT/connector-plugins" | jq -r '.[] | "\(.type)\t\(.class)\t\(.version)"' | column -t
  ;;

config)
  name=${1:-$CONNECTOR_NAME}
  title "Connector config: $name (passwords masked)"
  show "curl $CONNECT/connectors/$name/config"
  curl -s "$CONNECT/connectors/$name/config" | jq 'with_entries(if (.key | test("password")) then .value = "********" else . end)'
  ;;

topics)
  title "CDC topics: <prefix>.<schema>.<table>  (messages = end offset)"
  show "kafka-get-offsets --topic '${BASE}\\..*'"
  $DOCKER exec broker kafka-get-offsets --bootstrap-server broker:29092 --topic "${TOPIC_PREFIX}\\.${CAPTURE_SCHEMA}\\..*" 2>/dev/null |
    awk -F: '{printf "%-40s partition=%s  messages=%s\n", $1, $2, $3}'
  title "Retention of the CDC topics (-1 = keep forever)"
  t=$($DOCKER exec broker kafka-topics --bootstrap-server broker:29092 --list | grep "^${BASE}\." | head -1)
  show "kafka-configs --describe --entity-name $t"
  $DOCKER exec broker kafka-configs --bootstrap-server broker:29092 --describe --entity-type topics --entity-name "$t" | grep -E 'retention.ms|cleanup.policy' | head -3
  ;;

schema)
  t=$(tbl "${1:-}")
  subj="${BASE}.${t}-value"
  title "Schema Registry: $subj"
  show "curl $SR/subjects/$subj/versions"
  curl -s "$SR/subjects/$subj/versions" | jq -c '{versions: .}'
  show "curl $SR/subjects/${BASE}.${t}-key/versions/latest/schema"
  curl -s "$SR/subjects/${BASE}.${t}-key/versions/latest/schema" | jq -c '{key_fields: [.fields[] | {(.name): .type}]}'
  show "curl $SR/subjects/$subj/versions/latest/schema   (row image, same for before/after)"
  curl -s "$SR/subjects/$subj/versions/latest/schema" |
    jq -r '.fields[] | select(.name == "before") | .type[] | objects | .fields[] | "  \(.name)\t\(.type | tostring)"' | column -t -s$'\t'
  ;;

event)   # event <TABLE> [n] - last n events of a topic, pretty-printed
  t=$(tbl "${1:-}"); n=${2:-1}
  topic="${BASE}.${t}"
  end=$($DOCKER exec broker kafka-get-offsets --bootstrap-server broker:29092 --topic "$topic" 2>/dev/null | cut -d: -f3)
  start=$(( end > n ? end - n : 0 ))
  sc=$(scale_map "$t")
  title "Last $n event(s) of $topic (offsets $start..$((end - 1)))"
  show "kafka-avro-console-consumer --topic $topic --partition 0 --offset $start"
  $DOCKER exec schema-registry kafka-avro-console-consumer --bootstrap-server broker:29092 --topic "$topic" \
      --partition 0 --offset "$start" --max-messages "$n" --timeout-ms 15000 \
      --formatter-property schema.registry.url=http://schema-registry:8081 \
      --formatter-property print.key=true --formatter-property print.offset=true 2>/dev/null |
    grep -E '^Offset:' | while IFS=$'\t' read -r off k v; do
      echo "${B}${off}${N}"
      echo "key:"; jq --argjson sc "$sc" "$JQ_UNWRAP $JQ_DEC u | fix(\$sc[\"$t-key\"])" <<<"$k"
      echo "value:"
      if [ "$v" = null ]; then echo "null   <- TOMBSTONE"
      else jq --argjson sc "$sc" "$JQ_UNWRAP $JQ_DEC"' u | .before |= fix($sc[$t]) | .after |= fix($sc[$t])' --arg t "$t" <<<"$v"; fi
    done
  ;;

watch)   # watch [TABLE ...] - live, one line per change event (Ctrl-C to stop)
  if [ $# -eq 0 ]; then set -- ORDERS ORDER_ITEMS; fi
  topics=""; for t in "$@"; do topics+="${topics:+|}${BASE}\\.$(tbl "$t")"; done
  sc=$(scale_map $(for t in "$@"; do tbl "$t"; done))
  title "Watching live change events on: $* (Ctrl-C to stop)"
  $DOCKER exec schema-registry kafka-avro-console-consumer --bootstrap-server broker:29092 \
      --include "^(${topics})\$" --formatter-property schema.registry.url=http://schema-registry:8081 \
      --formatter-property print.key=true --formatter-property print.timestamp=true 2>/dev/null |
    grep --line-buffered -E '^CreateTime:' | while IFS=$'\t' read -r ts k v; do
      if [ "$v" = null ]; then
        printf '%s %s %-9s key=%s\n' "$(date +%T)" "${Y}" "TOMBSTONE" "$(jq -c --argjson sc "$sc" "$JQ_UNWRAP $JQ_DEC u | fix(\$sc._keys)" <<<"$k")${N}"
        continue
      fi
      jq -r --argjson sc "$sc" --arg now "$(date +%T)" --arg G "$G" --arg R "$R" --arg C "$C" --arg N "$N" "$JQ_UNWRAP $JQ_DEC"'
        (u | .source.table as $t | .before |= fix($sc[$t]) | .after |= fix($sc[$t])) as $e
        | ({"c":"INSERT","u":"UPDATE","d":"DELETE","r":"SNAPSHOT"}[$e.op] // $e.op) as $op
        | ({"INSERT":$G,"DELETE":$R}[$op] // $C) as $col
        | "\($now) \($col)\($op|.+"        "|.[0:9])\($N) \($e.source.table)  scn=\($e.source.scn)  "
          + (if $e.op == "u" then
               "changed: " + ([$e.after | to_entries[] | select(.value != $e.before[.key]) | "\(.key): \($e.before[.key]) -> \(.value)"] | join(", "))
             elif $e.op == "d" then "before: \($e.before | tostring)"
             else "after: \($e.after | tostring)" end)' <<<"$v"
    done
  ;;

insert)  # new order + 2 items in ONE transaction
  sql="DECLARE
  v_id NUMBER;
BEGIN
  INSERT INTO orders (customer_id, status, salesman_id, order_date)
  VALUES (1, 'Pending', 54, SYSDATE) RETURNING order_id INTO v_id;
  INSERT INTO order_items (order_id, item_id, product_id, quantity, unit_price) VALUES (v_id, 1, 1, 2, 499.99);
  INSERT INTO order_items (order_id, item_id, product_id, quantity, unit_price) VALUES (v_id, 2, 2, 1, 1299.00);
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('created ORDER_ID=' || v_id || ' with 2 items (1 transaction, 3 rows)');
END;
/"
  baseline >/dev/null
  title "INSERT in source ${CAPTURE_SCHEMA}"; showsql "$sql"; src_sql "$sql"
  ;;

update)  # update [ORDER_ID] [STATUS]
  id=${1:-$(last_order)}; st=${2:-Shipped}
  [ "$id" != 0 ] || { echo "no demo order yet - run: demo.sh insert"; exit 1; }
  sql="UPDATE orders SET status = '$st' WHERE order_id = $id;
UPDATE order_items SET quantity = quantity + 1 WHERE order_id = $id AND item_id = 1;
COMMIT;"
  title "UPDATE in source ${CAPTURE_SCHEMA}"; showsql "$sql"; src_sql "$sql"
  ;;

delete)  # delete [ORDER_ID] - FK ON DELETE CASCADE also removes the items
  id=${1:-$(last_order)}
  [ "$id" != 0 ] || { echo "no demo order to delete"; exit 1; }
  sql="-- the ORDER_ITEMS rows are removed by ON DELETE CASCADE
DELETE FROM orders WHERE order_id = $id;
COMMIT;"
  title "DELETE in source ${CAPTURE_SCHEMA}"; showsql "$sql"; src_sql "$sql"
  ;;

compare) # compare [TABLE] [ORDER_ID|WHERE-clause]
  t=$(tbl "${1:-}"); w=${2:-}
  if [ -z "$w" ]; then
    id=$(last_order); [ "$id" != 0 ] && w="order_id = $id" || w="1=1 FETCH FIRST 5 ROWS ONLY"
  elif [[ "$w" =~ ^[0-9]+$ ]]; then w="order_id = $w"; fi
  [[ "$t" =~ ^ORDER ]] || [[ "$w" != order_id* ]] || w="1=1 FETCH FIRST 5 ROWS ONLY"
  for s in "$CAPTURE_SCHEMA" "$SINK_SCHEMA"; do
    title "$s.$t  WHERE $w"
    rows=$(pdb_grid "SELECT * FROM $s.\"$t\" WHERE $w ORDER BY 1, 2;")
    [ -n "$rows" ] && echo "$rows" || echo "  (no rows)"
  done
  [ "$t" = ORDERS ] && echo && echo "  note: ORDER_DATE (Oracle DATE) arrives as epoch milliseconds -> NUMBER(19) in the sink"
  ;;

counts)
  title "Row counts: ${CAPTURE_SCHEMA} (source) vs ${SINK_SCHEMA} (sink)"
  q=""
  for t in $($DOCKER exec broker kafka-topics --bootstrap-server broker:29092 --list | grep "^${BASE}\." | sed 's/.*\.//'); do
    q+="SELECT '$t' AS table_name, (SELECT COUNT(*) FROM ${CAPTURE_SCHEMA}.\"$t\") AS source_rows, (SELECT COUNT(*) FROM ${SINK_SCHEMA}.\"$t\") AS sink_rows FROM dual UNION ALL "
  done
  pdb_sql "SET MARKUP CSV ON DELIMITER | QUOTE OFF
SELECT table_name, source_rows, sink_rows, CASE WHEN source_rows = sink_rows THEN 'OK' ELSE 'DIFF' END AS status FROM (${q% UNION ALL }) ORDER BY 1;" |
    grep -v '^Session altered' | grid | sed "s/ OK\$/ ${G}OK${N}/; s/ DIFF\$/ ${R}DIFF${N}/"
  ;;

lag)
  title "JDBC sink consumer group connect-${JDBC_SINK_NAME}: lag per topic"
  show "kafka-consumer-groups --describe --group connect-${JDBC_SINK_NAME}"
  $DOCKER exec broker kafka-consumer-groups --bootstrap-server broker:29092 --describe --group "connect-${JDBC_SINK_NAME}" 2>/dev/null |
    awk '/^GROUP|^connect-/ {printf "%-38s %-15s %-15s %s\n", $2, $4, $5, $6; if (/^connect-/) tot+=$6} END {print "TOTAL LAG: " tot}'
  ;;

load)    # load [N] - N orders (+1 item each), committed every 100
  n=${1:-500}
  sql="DECLARE
  TYPE t_ids IS TABLE OF NUMBER;
  prods t_ids; custs t_ids; v_id NUMBER; v_cust NUMBER; v_prod NUMBER;
BEGIN
  SELECT product_id  BULK COLLECT INTO prods FROM products;
  SELECT customer_id BULK COLLECT INTO custs FROM customers;
  FOR i IN 1..$n LOOP
    v_cust := custs(MOD(i, custs.COUNT) + 1);
    v_prod := prods(MOD(i, prods.COUNT) + 1);
    INSERT INTO orders (customer_id, status, order_date)
    VALUES (v_cust, 'Pending', SYSDATE) RETURNING order_id INTO v_id;
    INSERT INTO order_items (order_id, item_id, product_id, quantity, unit_price)
    VALUES (v_id, 1, v_prod, MOD(i, 5) + 1, 10 + MOD(i, 90));
    IF MOD(i, 100) = 0 THEN COMMIT; END IF;
  END LOOP;
  COMMIT;
  DBMS_OUTPUT.PUT_LINE('$n orders + $n items committed');
END;
/"
  baseline >/dev/null
  title "Burst: $n orders"; showsql "$sql"
  s=$SECONDS; src_sql "$sql"; echo "  source commit took $((SECONDS - s)) s"
  ;;

latency) # time from source COMMIT until the row is in the sink table
  m="LAT_$(date +%s)"
  title "End-to-end latency: Oracle COMMIT -> XStream -> Kafka -> JDBC sink -> Oracle"
  pdb_sql "DECLARE
  t0 TIMESTAMP; n NUMBER := 0;
BEGIN
  INSERT INTO ${CAPTURE_SCHEMA}.regions (region_name) VALUES ('$m');
  COMMIT;
  t0 := SYSTIMESTAMP;
  WHILE n = 0 AND SYSTIMESTAMP - t0 < INTERVAL '60' SECOND LOOP
    DBMS_SESSION.SLEEP(0.1);
    SELECT COUNT(*) INTO n FROM ${SINK_SCHEMA}.\"REGIONS\" WHERE \"REGION_NAME\" = '$m';
  END LOOP;
  DBMS_OUTPUT.PUT_LINE(CASE WHEN n = 1
    THEN 'row visible in ${SINK_SCHEMA}.REGIONS after ' || ROUND(EXTRACT(SECOND FROM SYSTIMESTAMP - t0) * 1000) || ' ms'
    ELSE 'row NOT replicated within 60 s' END);
  DELETE FROM ${CAPTURE_SCHEMA}.regions WHERE region_name = '$m';
  COMMIT;
END;
/" | grep -v '^Session altered'
  ;;

pause-sink|resume-sink)
  action=${cmd%-sink}
  title "${action^} ${JDBC_SINK_NAME}"
  show "curl -X PUT $CONNECT/connectors/${JDBC_SINK_NAME}/$action"
  curl -s -X PUT "$CONNECT/connectors/${JDBC_SINK_NAME}/$action"
  sleep 3; connector_states
  ;;

stop-connect)
  title "Simulating an outage of the Connect worker (both connectors stop)"
  show "docker stop connect"; $DOCKER stop connect >/dev/null && echo "connect stopped"
  ;;

start-connect)
  title "Starting the Connect worker - connectors resume from their stored positions"
  show "docker start connect"; $DOCKER start connect >/dev/null
  printf 'waiting for connectors'; wait_running
  ;;

evolve)  # add a column in the source -> new schema version -> sink adds the column
  sql="ALTER TABLE orders ADD (priority VARCHAR2(10));
UPDATE orders SET priority = 'HIGH' WHERE order_id = $(last_order);
COMMIT;"
  [ "$(last_order)" != 0 ] || { echo "run demo.sh insert first"; exit 1; }
  title "Schema evolution in source ${CAPTURE_SCHEMA}.ORDERS"; showsql "$sql"; src_sql "$sql"
  ;;

sinkddl) # sinkddl [TABLE] - DDL the sink generated with auto.create
  t=$(tbl "${1:-}")
  title "DDL of ${SINK_SCHEMA}.$t (created by the JDBC sink, auto.create=true)"
  pdb_sql "SET LONG 10000 PAGESIZE 0 HEADING OFF
EXEC DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
SELECT DBMS_METADATA.GET_DDL('TABLE', '$t', '${SINK_SCHEMA}') FROM dual;" | grep -v -e '^Session altered' -e '^$'
  ;;

reset)   # remove everything the demo added; keep the pipeline running
  b=$(baseline)
  title "Reset: delete demo orders (> ORDER_ID $b), drop demo column, resume sink"
  has_col=$(val "SELECT COUNT(*) FROM dba_tab_columns WHERE owner='${CAPTURE_SCHEMA}' AND table_name='ORDERS' AND column_name='PRIORITY';")
  sql="DELETE FROM orders WHERE order_id > $b;
COMMIT;"
  [ "$has_col" = 1 ] && sql+=$'\nALTER TABLE orders DROP COLUMN priority;'
  showsql "$sql"; src_sql "$sql"
  if [ "$has_col" = 1 ] || [ "$(val "SELECT COUNT(*) FROM dba_tab_columns WHERE owner='${SINK_SCHEMA}' AND table_name='ORDERS' AND column_name='PRIORITY';")" = 1 ]; then
    # auto.evolve only adds columns, so the sink copy is dropped explicitly
    showsql "ALTER TABLE ${SINK_SCHEMA}.\"ORDERS\" DROP COLUMN \"PRIORITY\";   -- sink never drops columns itself"
    pdb_sql "ALTER TABLE ${SINK_SCHEMA}.\"ORDERS\" DROP COLUMN \"PRIORITY\";" >/dev/null
  fi
  [ "$($DOCKER inspect -f '{{.State.Running}}' connect)" = true ] || $DOCKER start connect >/dev/null
  curl -s -X PUT "$CONNECT/connectors/${JDBC_SINK_NAME}/resume" >/dev/null 2>&1 || true
  printf 'waiting for connectors'; wait_running
  echo "waiting 20 s for the sink to apply the deletes"; sleep 20
  "$SELF" counts
  ;;

*)
  cat <<EOF
Usage: $0 <command> [args]         (full walkthrough: DEMO.md)

 Tour         status | oracle | plugins | config [NAME] | topics | schema [TABLE] | event [TABLE] [N] | sinkddl [TABLE]
 Live CDC     watch [TABLE...]  (run in its own window)
              insert | update [ID] [STATUS] | delete [ID] | compare [TABLE] [ID|WHERE]
 Proof        counts | lag | latency
 Scenarios    load [N] | pause-sink | resume-sink | stop-connect | start-connect | evolve
 Cleanup      reset
EOF
  ;;
esac
