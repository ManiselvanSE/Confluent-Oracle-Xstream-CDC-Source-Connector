# Oracle XE 21c → XStream CDC → Confluent Platform 8.x (Terraform)

Terraform project that turns **one existing EC2 host** into a complete Oracle
change-data-capture lab, all running in Docker:

- **Oracle Database 21c Express Edition** with XStream enabled (ARCHIVELOG,
  supplemental logging, capture process and outbound server) and the
  `ORDERMGMT` sample schema.
- **Confluent Platform 8.3** in KRaft mode (no ZooKeeper): a broker and Schema Registry.
- **Kafka Connect** worker built with the **Confluent Oracle XStream CDC
  Source connector** and Oracle Instant Client.
- The **connector** itself, plus a **validation script** that proves an
  `INSERT` in Oracle arrives as an Avro event in Kafka.
- **Confluent Control Center** (CP 8.x "next-gen" 2.5, with its Prometheus and
  Alertmanager) on port 9021. Optional: `enable_control_center`.
- A **JDBC sink connector** that replays the CDC topics into a second schema,
  `ORDERMGMT_SINK`, in the same PDB (Oracle → Kafka → Oracle), including
  updates and deletes. Optional: `enable_jdbc_sink`.

The layout follows the reference setup in
[ora0600/confluent-new-cdc-connector/oraclexe21c](https://github.com/ora0600/confluent-new-cdc-connector/tree/main/oraclexe21c),
with the connector running on a self-managed CP 8 stack instead of Confluent Cloud.

---

## Architecture

```
                         EC2 host (Amazon Linux 2023, x86_64)
 ┌───────────────────────────────────────────────────────────────────────────┐
 │  docker compose project "cdc-xstream"                                     │
 │                                                                           │
 │  ┌──────────────────────┐   XStream Out    ┌───────────────────────────┐  │
 │  │ oracle21c            │  (OCI, port 1521)│ connect                   │  │
 │  │ Oracle XE 21c        │ ───────────────▶ │ cp-kafka-connect 8.3.2    │  │
 │  │  CDB  XE             │                  │ + XStream CDC connector   │  │
 │  │  PDB  XEPDB1         │                  │ + Oracle Instant Client 21│  │
 │  │  capture CONFLUENT_  │                  └────────────┬──────────────┘  │
 │  │   XOUT1 → outbound   │                               │ Avro records    │
 │  │   server XOUT        │                               ▼                 │
 │  └──────────────────────┘   ┌──────────────────┐  ┌──────────────────┐    │
 │                             │ schema-registry  │◀─│ broker (KRaft)   │    │
 │                             │ :8081            │  │ cp-server :9092  │    │
 │                             └──────────────────┘  └──────────────────┘    │
 └───────────────────────────────────────────────────────────────────────────┘
       ▲ terraform (your laptop) connects over SSH and runs each stage
```

With the optional parts enabled, the same compose project also runs:

```
 connect ── JDBC sink (JDBCSINK0) ──▶ oracle21c XEPDB1.ORDERMGMT_SINK.<TABLE>
 broker  ── OTLP metrics ──▶ prometheus :9090 ◀── control-center :9021 ──▶ alertmanager :9093
```

Topics created by the connector are named `<topic_prefix>.<SCHEMA>.<TABLE>`,
for example `XEPDB1.ORDERMGMT.ORDERS`.

---

## Repository layout

```
.
├── versions.tf                 # Terraform >= 1.5, no providers needed
├── variables.tf                # every tunable (host, versions, passwords, tables…)
├── main.tf                     # rendered config + the 7 stages (terraform_data)
├── outputs.tf                  # ssh / tunnel / validate commands, topic list
├── terraform.tfvars.example    # copy to terraform.tfvars
├── DEMO.md                     # customer demo runbook (talk track + commands)
├── templates/
│   ├── env.tftpl               # → <remote_dir>/.env  (single source of config on the host)
│   ├── docker-compose.yml      # → <remote_dir>/docker-compose.yml
│   └── 03_create_outbound.sql.tftpl  # → XStream outbound server for your table list
└── files/
    ├── host/                   # → <remote_dir>/bin  (run on the host with sudo)
    │   ├── 00_preflight.sh     # OS/arch/disk/RAM/internet checks, auto-grows root FS
    │   ├── 01_host_prep.sh     # docker, compose, buildx, sysctl, ulimits, swap
    │   ├── 02_deploy_oracle.sh # start XE, wait for DB, run CDC preparation
    │   ├── 03_deploy_confluent.sh # build Connect image, start broker/SR/connect
    │   ├── 04_deploy_connector.sh # PUT connector config, wait for RUNNING
    │   ├── 05_deploy_jdbc_sink.sh # sink schema + JDBC sink connector
    │   ├── validate.sh         # checks + end-to-end insert/delete → Kafka → sink test
    │   ├── demo.sh             # presenter helper used by DEMO.md (watch, insert, lag, reset…)
    │   └── purge_archivelogs.sh# RMAN cleanup of old archive logs
    ├── oracle/                 # → <remote_dir>/oracle (mounted into the container)
    │   ├── 00_setup_cdc.sh     # orchestrates DB preparation (runs once)
    │   ├── 01_enable_xstream.sql
    │   └── 02_xstream_users.sql
    ├── connect/
    │   └── Dockerfile          # cp-kafka-connect + Instant Client + XStream + JDBC connectors
    └── c3/                     # → <remote_dir>/c3: Prometheus/Alertmanager configs for Control Center
                                #   (from confluentinc/cp-all-in-one 8.3.2-post)
```

---

## Prerequisites

| What | Why |
|------|-----|
| Terraform ≥ 1.5 on your laptop | runs the stages over SSH |
| An x86_64 EC2 host running Amazon Linux 2023 | the Oracle XE image is amd64 only |
| **Root volume ≥ 60 GB (100 GB recommended)** | XE image ≈ 11 GB unpacked, plus datafiles, archive logs and CP images |
| RAM ≥ 8 GB (16 GB / `t3.xlarge` recommended) | XE ≈ 2.5 GB, broker 1 GB, Connect 1.5 GB, SR 0.4 GB heaps |
| Security group: TCP 22 from your IP | Terraform and the SSH tunnel |
| Outbound HTTPS from the host | Oracle registry, Docker Hub, Confluent Hub, GitHub |

> The target host `13.233.21.126` is a `t3.xlarge` (4 vCPU / 16 GB) with a
> 100 GB root volume. The instance has no Elastic IP, so its public IP changes
> after every stop/start. Update `host` in `terraform.tfvars` when that happens,
> or attach an Elastic IP.

---

## Step-by-step

### Step 1: Enlarge the root EBS volume

The preflight stops with a clear error if there is not enough free space.

1. AWS Console → EC2 → Instances → select the instance → **Storage** tab →
   click the root volume ID.
2. **Actions → Modify volume** → size `100` GiB (gp3) → **Modify**.
3. Nothing else is required. `00_preflight.sh` runs `growpart` and
   `xfs_growfs` on the host, so the filesystem grows automatically.


### Step 2: Configure

```bash
cd oracle_xstr
cp terraform.tfvars.example terraform.tfvars
vi terraform.tfvars      # host, ssh key path, passwords
```

Only `host`, `ssh_private_key_path` and the three passwords are required.
Everything else has defaults matching the reference setup (`XE` / `XEPDB1`,
`C##GGADMIN`, `C##CFLTUSER`, outbound `XOUT`, schema `ORDERMGMT`, the 13
tables and topic prefix `XEPDB1`).

`terraform.tfvars` holds passwords and is git-ignored.

### Step 3: Initialise and review the plan

```bash
terraform init
terraform plan
```

The plan shows six `terraform_data` resources. No cloud resources are
created; each stage is a set of SSH provisioners against the existing host.

### Step 4: Apply

```bash
terraform apply
```

The first run takes **roughly 30–45 minutes**, mostly the Oracle image pull
and database creation. Every stage streams its log to your terminal. Each
stage is described below.

#### Stage 0: `terraform_data.teardown`
Does nothing on create. It exists so that `terraform destroy` runs
`docker compose down` on the host. It is created first and destroyed last.
Data volumes are kept unless `destroy_purges_data = true`.

#### Stage 1: `terraform_data.bootstrap`: upload, preflight, host prep
1. Creates `<remote_dir>` (default `/home/ec2-user/cdc-xstream`) and uploads
   `bin/`, `oracle/`, `connect/`, `docker-compose.yml`, the rendered
   `.env`, the outbound-server SQL and the connector JSON. The files holding
   passwords get mode `600`.
2. `00_preflight.sh`: checks Amazon Linux, **x86_64**, and grows the root
   partition if the EBS volume was enlarged. Then it checks for
   **≥ 40 GB free disk**, ≥ 8 GB RAM and HTTPS reachability of every
   download source.
3. `01_host_prep.sh`: installs Docker from the AL2023 repo, the **Docker
   Compose v5** and **Buildx** CLI plugins, log rotation for containers,
   `vm.max_map_count`/`nofile` limits and a swap file. It also adds
   `ec2-user` to the `docker` group.

It re-runs whenever any uploaded file changes. Everything in it is idempotent.

#### Stage 2: `terraform_data.oracle`: Oracle XE 21c + XStream
`02_deploy_oracle.sh` on the host:
1. Downloads the reference repo's `ORDERMGMT` sample scripts (user, data
   model, ~400 KB data load, `produce_orders` generator) from a **pinned
   commit** into `oracle/sample/`.
2. Pulls `container-registry.oracle.com/database/express:21.3.0-xe` and
   starts it. `oradata` is a named volume, so data survives restarts.
3. Waits for `DATABASE IS READY TO USE!`. The first boot creates the database.
4. Runs `00_setup_cdc.sh` inside the container. It runs **once**, recorded
   by a marker file in the volume:

| # | What | Why |
|---|------|-----|
| 1 | `enable_goldengate_replication=TRUE` | XStream is licensed/enabled through this parameter |
| 1 | `streams_pool_size=256M` | memory for capture + outbound server |
| 1 | Recovery area in `/opt/oracle/oradata/fast_recovery_area`, 20 GB | archive logs live in the persistent volume and are size-capped |
| 1 | Redo logs recreated at 512 MB (3 groups) | fewer log switches under CDC load (reference used 2 GB; reduced for disk) |
| 1 | `ADD SUPPLEMENTAL LOG DATA` + `(ALL) COLUMNS` | redo must contain full before/after images for CDC |
| 2 | Restart → `ARCHIVELOG` → open all PDBs + save state | capture reads archived redo; restart also applies the streams pool |
| 3 | `ORDERMGMT` schema + data in `XEPDB1` | sample tables to stream |
| 4 | `C##GGADMIN`: common user, `DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE` (+DBA as in the reference) | owns capture/outbound |
| 4 | `C##CFLTUSER`: `CREATE SESSION`, `SET CONTAINER`, `SELECT_CATALOG_ROLE`, `SELECT/LOCK/FLASHBACK ANY TABLE` | least-privilege user the connector logs in with (snapshot + streaming) |
| 5 | `DBMS_XSTREAM_ADM.CREATE_OUTBOUND` (`XOUT`, capture `CONFLUENT_XOUT1`, source container `XEPDB1`, 13 tables), checkpoint retention, `max_sga_size=256`, `ALTER_OUTBOUND connect_user=C##CFLTUSER` | the XStream outbound server the connector attaches to |

5. Re-applies `03_create_outbound.sql` on every run. It is idempotent and
   keeps capture parameters in sync with `terraform.tfvars`.

#### Stage 3: `terraform_data.confluent`: Confluent Platform 8.3 + Connect image
`03_deploy_confluent.sh` on the host:
1. Downloads **Oracle Instant Client 21.19** (Basic, x64) and the **XStream
   CDC connector 1.6.0** zip from Confluent Hub into `connect/stage/`.
2. Copies `ojdbc8.jar` and `xstreams.jar` from the Instant Client into the
   connector's `lib/`. The connector uses the XStream Out API over the
   **OCI (thick) driver**, so the jars and native libraries must be the same version.
3. Builds `cdc-xstream/connect:8.3.2-xstream-1.6.0` from
   `files/connect/Dockerfile`: `cp-kafka-connect` + `libaio` + Instant Client
   with `LD_LIBRARY_PATH` + the plugin.
   The **JDBC connector 10.9.9** is downloaded and baked in as well. It
   bundles its own `ojdbc8`. The image tag is
   `cdc-xstream/connect:8.3.2-xstream-1.6.0-jdbc-10.9.9`.
4. Starts `broker` (cp-server, KRaft combined broker/controller, RF 1),
   `schema-registry` and `connect`. It then waits until Kafka answers,
   Schema Registry answers, Connect REST is up and both plugins are listed.
5. With `enable_control_center = true`, it also starts `prometheus`,
   `alertmanager` and `control-center` (compose profile `c3`). CP 8.x Control
   Center has no metrics topics of its own: the broker's telemetry reporter
   pushes metrics to Prometheus over OTLP, and Control Center reads them from
   there. The broker settings for this come from Confluent's
   `cp-all-in-one` 8.3.2 compose file.

#### Stage 4: `terraform_data.connector`: create the connector
`04_deploy_connector.sh` sends `PUT /connectors/XSTREAMCDC0/config`
(create-or-update) and waits for the connector and task to be `RUNNING`.
Key settings, rendered from variables in `main.tf`:

```json
{
  "connector.class": "io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector",
  "database.hostname": "oracle21c", "database.port": "1521",
  "database.user": "C##CFLTUSER",
  "database.dbname": "XE", "database.service.name": "XE", "database.pdb.name": "XEPDB1",
  "database.out.server.name": "XOUT",
  "table.include.list": "ORDERMGMT[.](ORDERS|ORDER_ITEMS|...|REGIONS)",
  "topic.prefix": "XEPDB1",
  "snapshot.mode": "initial",
  "schema.history.internal.kafka.topic": "__orcl-schema-changes.XEPDB1",
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter": "io.confluent.connect.avro.AvroConverter"
}
```

`snapshot.mode=initial` first copies the existing rows of every table, then
streams changes. Without `confluent_license`, the connector runs on a
30-day trial.

#### Stage 5: `terraform_data.jdbc_sink`: Kafka → Oracle
`05_deploy_jdbc_sink.sh`:
1. Creates the user `ORDERMGMT_SINK` in `XEPDB1` (or resets its password)
   with `CREATE SESSION` / `CREATE TABLE` and quota on `USERS`.
2. Sends `PUT /connectors/JDBCSINK0/config` and waits for `RUNNING`.

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "topics": "XEPDB1.ORDERMGMT.ORDERS,...,XEPDB1.ORDERMGMT.REGIONS",
  "connection.url": "jdbc:oracle:thin:@//oracle21c:1521/XEPDB1",
  "connection.user": "ORDERMGMT_SINK",
  "insert.mode": "upsert", "pk.mode": "record_key", "delete.enabled": "true",
  "auto.create": "true", "auto.evolve": "true",
  "table.name.format": "ORDERMGMT_SINK.${topic}",
  "transforms": "unwrap,route",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.delete.tombstone.handling.mode": "tombstone",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": "XEPDB1\\.ORDERMGMT\\.(.*)", "transforms.route.replacement": "$1"
}
```

- `unwrap` turns the CDC envelope (`before`/`after`/`source`) into the plain
  row. For a delete, the source sends a delete event followed by a
  tombstone. `unwrap` drops the delete event and keeps the tombstone, which
  makes the sink run `DELETE ... WHERE <pk>`.
- `route` strips the topic prefix, so `XEPDB1.ORDERMGMT.ORDERS` → table `ORDERS`.
- The record key holds the source primary key, so `upsert` + `record_key`
  keeps the target in sync however often a row changes.
- Tables are created automatically on first use. Oracle `DATE` columns
  arrive as epoch milliseconds (`io.debezium.time.Timestamp`) and are stored
  as `NUMBER(19)` in the sink tables.

Setting `enable_jdbc_sink = false` and re-applying deletes the connector. The
sink schema and its tables are kept.

#### Stage 6: `terraform_data.validation`: prove it works
`validate.sh` prints PASS/FAIL for each check and exits non-zero if any fail:

1. All four containers are running.
2. Oracle: `ARCHIVELOG`, supplemental logging MIN + ALL, GoldenGate
   replication on, PDB `READ WRITE`.
3. XStream: outbound `XOUT` exists with connect user `C##CFLTUSER`, the
   capture and apply processes are `ENABLED`, and a client is attached
   (`GV$XSTREAM_OUTBOUND_SERVER`).
4. Kafka reachable, Schema Registry reachable, plugin installed, connector
   and task `RUNNING`, CDC topics exist.
5. Control Center (if enabled): the three containers are running, Prometheus
   is ready and the UI answers on 9021.
6. JDBC sink (if enabled): connector and task `RUNNING`, and row counts
   match between `ORDERMGMT` and `ORDERMGMT_SINK` for every table.
7. **End to end**: inserts `REGIONS.REGION_NAME='VALIDATE_<epoch>'`, reads
   `XEPDB1.ORDERMGMT.REGIONS` with `kafka-avro-console-consumer`, looks for
   the marker, checks that the row reached `ORDERMGMT_SINK.REGIONS`, then
   deletes it and checks that the delete reached the sink too.

Re-run it any time:

```bash
terraform output -raw validate | sh
# or
terraform apply -replace='terraform_data.validation[0]'
```

### Step 5: Use the environment

```bash
terraform output ssh          # log in
terraform output ssh_tunnel   # forward 1521/9092/8081/8083 (+9021) to your laptop
```

With the tunnel open, **Control Center** is at http://localhost:9021. It shows
the cluster, topics and messages, Schema Registry subjects and both connectors
(`XSTREAMCDC0`, `JDBCSINK0`) under **Connect**. Broker metrics are collected
every 60 s, so the charts fill in after a couple of minutes.

Ports are published on `127.0.0.1` only (`bind_address`). Reach them through
the tunnel, or set `bind_address = "0.0.0.0"` and restrict the security group.

On the host (`cd ~/cdc-xstream`):

```bash
sudo docker compose ps                                     # service status
sudo docker compose logs -f connect                        # connector logs
curl -s localhost:8083/connectors/XSTREAMCDC0/status | jq  # connector state
sudo docker exec broker kafka-topics --bootstrap-server broker:29092 --list

# Watch order events live
sudo docker exec -it schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker:29092 --topic XEPDB1.ORDERMGMT.ORDERS \
  --property schema.registry.url=http://schema-registry:8081

# Compare source and sink
sudo docker exec -it oracle21c sqlplus ordermgmt_sink/<sink_schema_password>@//localhost:1521/XEPDB1
SQL> select count(*) from orders;

# Generate load: one new order every 5 s for 5 h (procedure from the sample scripts)
sudo docker exec -it oracle21c sqlplus ordermgmt/kafka@//localhost:1521/XEPDB1
SQL> exec produce_orders
```

### Step 6: Day-2 operations

| Task | How |
|------|-----|
| Change connector settings / tables | edit `terraform.tfvars` → `terraform apply` (connector is updated in place) |
| Add tables to capture | add to `capture_tables`, then add them to the existing outbound server with `DBMS_XSTREAM_ADM.ADD_TABLE_RULES` ([reference guide](https://github.com/ora0600/confluent-new-cdc-connector/blob/main/adding_tables.md)). `CREATE_OUTBOUND` only runs when `XOUT` does not exist yet |
| Upgrade CP / connector | change `cp_version` / `xstream_connector_version` → `terraform apply` rebuilds Connect |
| Change topic retention | `topic_retention_ms` (default `-1` = keep forever) → `terraform apply`. Sets the broker default, the source connector's `topic.creation.default.retention.ms` and `retention.ms` on every existing CDC topic |
| Turn Control Center / JDBC sink off or on | `enable_control_center` / `enable_jdbc_sink` → `terraform apply` |
| Re-sync the sink from scratch | `curl -X PUT localhost:8083/connectors/JDBCSINK0/stop`, `curl -X DELETE localhost:8083/connectors/JDBCSINK0/offsets`, drop the `ORDERMGMT_SINK` tables, then `curl -X PUT localhost:8083/connectors/JDBCSINK0/resume`. The topics are replayed from the beginning |
| Free archive-log space | `sudo bash bin/purge_archivelogs.sh 1`. RMAN keeps logs still needed by capture |
| Restart everything | `sudo docker compose restart` (DB and Kafka data are on named volumes) |
| Diagnose XStream | the connector zip ships `assets/orclcdc_diag.sql` in `connect/stage/confluentinc-kafka-connect-oracle-xstream-cdc-source-*/` |

### Step 7: Tear down

```bash
terraform destroy
```

This stops and removes the containers. The Oracle and Kafka volumes are kept,
so a later `apply` reuses the existing database. Set
`destroy_purges_data = true` (and `apply` once) to also delete the volumes and
`<remote_dir>`.

---

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `PREFLIGHT FAILED: not enough disk` | enlarge the EBS volume (Step 1) and re-apply |
| Oracle stage waits a long time | first DB creation takes 10–25 min on 2 vCPUs. Check `sudo docker logs -f oracle21c` |
| Connector `FAILED` with `UnsatisfiedLinkError` / `ocijdbc21` | Instant Client not on `LD_LIBRARY_PATH`, or jars from a different version. Rebuild via `terraform apply -replace=terraform_data.confluent` |
| Connector `FAILED`: outbound server not found / not attachable | check `SELECT server_name, connect_user, status FROM dba_xstream_outbound o JOIN dba_apply a ON a.apply_name = o.server_name;` as SYS in the CDB |
| Connector RUNNING but no data | capture may be waiting for redo; see [waiting_for_redo_state.md](https://github.com/ora0600/confluent-new-cdc-connector/blob/main/waiting_for_redo_state.md). Check `SELECT state FROM v$xstream_capture;` |
| DB hangs, `ORA-00257` | recovery area full: run `bin/purge_archivelogs.sh` or raise `recovery_area_size_gb` |
| OOM kills on 8 GB host | lower heaps in `terraform.tfvars` or move to `t3.xlarge` |

---

## Differences from the reference repo

| Reference (`oraclexe21c`) | This project | Why |
|---|---|---|
| Creates a new EC2 via user-data | Configures an **existing** host over SSH | target host already exists |
| Connector runs against **Confluent Cloud** | Local **CP 8.3.2** (KRaft) broker, Schema Registry and Connect | self-managed platform requested |
| Instant Client **arm64** 19.26 | Instant Client **x64** 21.19 | host is x86_64 |
| `01_setup_database.sql` uses 23ai `FREE` paths | paths derived from `v$logfile` / XE layout | the `FREE` paths fail on XE |
| Redo logs 3 × 2 GB | 3 × 512 MB (configurable) | 8 GB-RAM / small-disk host |
| Setup scripts mounted in `/opt/oracle/scripts/setup` and also run by hand | mounted in `/opt/oracle/scripts/cdc`, run once via marker | avoids the image auto-running them a second time |
| Connector logs in as `C##GGADMIN` (DBA) | Connector logs in as `C##CFLTUSER`, via `ALTER_OUTBOUND connect_user` | least privilege, as in the Confluent docs |
| String/JSON converters | Avro + Schema Registry | uses the local Schema Registry |
| Legacy `docker-compose` 1.21 | Docker Compose v5 plugin + Buildx | current tooling on AL2023 |

## Security notes (lab setup)

- Passwords are in `terraform.tfvars`, in Terraform state, and on the host in
  `.env` / `connector/xstream-cdc.json` (mode 600). Keep state private, or
  use a remote backend with encryption.
- `C##GGADMIN` gets `DBA`, as in the reference. Remove it for anything beyond a lab.
- Kafka, Schema Registry and Connect run without TLS or auth and are bound to
  `127.0.0.1` by default.
- The sample user password `ordermgmt/kafka` is hard-coded in the reference sample scripts.
