#!/bin/bash
# Runs INSIDE the oracle21c container (docker exec) as the oracle OS user.
# Prepares Oracle XE 21c for the Confluent XStream CDC connector.
# Idempotent: a marker file in the persistent oradata volume records completion.
set -euo pipefail

DIR=/opt/oracle/scripts/cdc
MARKER=/opt/oracle/oradata/.cdc_setup_done
PDB="${ORACLE_PDB_NAME:-XEPDB1}"
export ORACLE_SID=XE

if [ -f "$MARKER" ]; then
  echo "CDC setup already completed on $(cat "$MARKER") - skipping"
  exit 0
fi

log() { echo "[$(date +%H:%M:%S)] $*"; }

# q <connect-string> <sql>  -> prints a single value
q() {
  sqlplus -s -L "$1" <<EOF | tr -d '[:space:]'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF
$2
EXIT
EOF
}

SYS_CDB="/ as sysdba"
SYS_PDB="sys/\"${ORACLE_PWD}\"@//localhost:1521/${PDB} as sysdba"
ADMIN_CDB="${CDC_ADMIN_USER}/\"${CDC_ADMIN_PWD}\"@//localhost:1521/XE"

log "[1/6] XStream parameters, redo logs, recovery area, supplemental logging"
mkdir -p /opt/oracle/oradata/fast_recovery_area
sqlplus -s -L "$SYS_CDB" @"$DIR/01_enable_xstream.sql" "${REDO_LOG_SIZE_MB:-0}" "${RECOVERY_AREA_SIZE_GB:-20}" </dev/null

log "[2/6] Restart database (ARCHIVELOG mode + new streams pool size)"
MODE=$(q "$SYS_CDB" "SELECT log_mode FROM v\$database;")
ARCHIVELOG_STMT=""
[ "$MODE" = "NOARCHIVELOG" ] && ARCHIVELOG_STMT="ALTER DATABASE ARCHIVELOG;"
sqlplus -s -L "$SYS_CDB" <<EOF
WHENEVER SQLERROR EXIT SQL.SQLCODE
SHUTDOWN IMMEDIATE
STARTUP MOUNT
${ARCHIVELOG_STMT}
ALTER DATABASE OPEN;
WHENEVER SQLERROR CONTINUE
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER PLUGGABLE DATABASE ALL SAVE STATE;
SELECT log_mode FROM v\$database;
EXIT
EOF

log "[3/6] Sample schema ${CAPTURE_SCHEMA} (from the reference repo)"
if [ "$(q "$SYS_PDB" "SELECT COUNT(*) FROM dba_users WHERE username='${CAPTURE_SCHEMA}';")" = "0" ]; then
  sqlplus -s -L "$SYS_PDB" @"$DIR/sample/02_create_user.sql" </dev/null
  for f in 03_create_schema_datamodel.sql 04_load_data.sql 06_data_generator.sql; do
    log "      running $f"
    sqlplus -s -L "ordermgmt/kafka@//localhost:1521/${PDB}" @"$DIR/sample/$f" </dev/null > "/tmp/$f.log" 2>&1
    errors=$(grep -c "ORA-" "/tmp/$f.log" || true)
    log "      $f finished ($errors ORA- messages, log: /tmp/$f.log in the container)"
  done
else
  log "      schema already exists - skipping"
fi

log "[4/6] XStream admin (${CDC_ADMIN_USER}) and connect (${CDC_USER}) users"
sqlplus -s -L "$SYS_CDB" @"$DIR/02_xstream_users.sql" "$CDC_ADMIN_USER" "$CDC_ADMIN_PWD" "$CDC_USER" "$CDC_USER_PWD" </dev/null

log "[5/6] XStream outbound server"
sqlplus -s -L "$ADMIN_CDB" @"$DIR/03_create_outbound.sql" </dev/null

log "[6/6] Done"
date -u +%Y-%m-%dT%H:%M:%SZ > "$MARKER"
