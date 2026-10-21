#!/usr/bin/env bash
# Delete archived redo logs older than N days (default 1) to free the recovery area.
# RMAN refuses to delete logs the XStream capture still needs (RMAN-08137),
# so this is safe to run from cron, e.g.:
#   0 * * * * sudo bash /home/ec2-user/cdc-xstream/bin/purge_archivelogs.sh 1
set -euo pipefail
DAYS=${1:-1}
docker exec -i -e ORACLE_SID=XE oracle21c bash -c "rman target / <<EOF
DELETE NOPROMPT ARCHIVELOG ALL COMPLETED BEFORE 'SYSDATE-${DAYS}';
EXIT
EOF"
