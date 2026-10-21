# Customer demo script: Oracle XStream CDC → Confluent Platform → Oracle (JDBC sink)

A presenter runbook for walking a customer through this lab, in order. Each act has:

- **Goal**: what the customer should take away
- **Say**: the talk track (adapt freely)
- **Do**: the exact commands
- **Expect**: what appears on screen (captured from this lab)
- **Point out**: details worth highlighting
- **If asked**: likely questions and short answers

The commands use the helper `~/cdc-xstream/bin/demo.sh` on the host (uploaded by Terraform with the other `bin/` scripts). Every helper command first prints the SQL, REST call or CLI it runs, in yellow, so the audience sees the real tooling and not a black box.

| | |
|---|---|
| **Full run** | ~70 min + Q&A (Acts 0–13) |
| **Short run (30 min)** | Acts 0, 2, 5, 7, 9, 11, 13 |
| **Audience** | DBAs, data/platform engineers, architects |
| **Key messages** | 1. Committed changes flow from Oracle in about 3 s, straight from the redo log, with no triggers and no polling. 2. Kafka decouples source and target; each side can stop and catch up without losing data. 3. Everything (DB prep, platform, connectors) is code: one `terraform apply`. |

---

## Contents
- [Before the demo](#before-the-demo)
- [Act 0: Opening and architecture](#act-0--opening-and-architecture-5-min)
- [Act 1: Infrastructure as code](#act-1--infrastructure-as-code-5-min)
- [Act 2: Runtime tour](#act-2--runtime-tour-3-min)
- [Act 3: Oracle source readiness](#act-3--oracle-source-readiness-5-min)
- [Act 4: Kafka Connect and the XStream source connector](#act-4--kafka-connect-and-the-xstream-source-connector-5-min)
- [Act 5: Topics, schemas and a change event](#act-5--topics-schemas-and-a-change-event-7-min)
- [Act 6: Control Center tour](#act-6--control-center-tour-5-min)
- [Act 7: Live CDC (INSERT, UPDATE, DELETE)](#act-7--live-cdc-insert-update-delete-10-min)
- [Act 8: Inside the JDBC sink](#act-8--inside-the-jdbc-sink-5-min)
- [Act 9: Proof (row counts and latency)](#act-9--proof-row-counts-and-latency-3-min)
- [Act 10: Burst load and back-pressure](#act-10--burst-load-and-back-pressure-5-min)
- [Act 11: Resilience (Connect outage)](#act-11--resilience-connect-outage-5-min)
- [Act 12: Schema evolution](#act-12--schema-evolution-5-min)
- [Act 13: Wrap-up and production considerations](#act-13--wrap-up-and-production-considerations-5-min)
- [After the demo](#after-the-demo)
- [If something goes wrong](#if-something-goes-wrong)
- [Appendix A: helper command reference](#appendix-a--helper-command-reference)
- [Appendix B: the same steps without the helper](#appendix-b--the-same-steps-without-the-helper)

---

## Before the demo

### The day before
1. Confirm the EC2 instance is running. The public IP is **not** an Elastic IP, so it changes after a stop/start. If it changed, update `host` in `terraform.tfvars` and run `terraform apply`. Nothing is rebuilt except the file upload stage.
2. Do a full dry run of this script, then run `demo.sh reset` at the end.

### 30 minutes before
Run these on your laptop, in `oracle_xstr/`:

```bash
terraform output -raw ssh          # SSH command
terraform output -raw ssh_tunnel   # port-forward command (Oracle 1521, Kafka 9092, SR 8081, Connect 8083, C3 9021)
```

1. **Open the tunnel** in its own terminal and leave it running:
   ```bash
   ssh -i /Users/maniselvan/Mani/mani_in.pem -N -L 1521:localhost:1521 -L 9092:localhost:9092 \
       -L 8081:localhost:8081 -L 8083:localhost:8083 -L 9021:localhost:9021 ec2-user@<host>
   ```
   If port 1521 is busy locally (a local Oracle, for example), drop `-L 1521:...`. Only SQL Developer needs it.

2. **Health check and clean slate** on the host:
   ```bash
   ssh -i /Users/maniselvan/Mani/mani_in.pem ec2-user@<host>
   alias demo=~/cdc-xstream/bin/demo.sh
   bash ~/cdc-xstream/bin/validate.sh      # expect: "== Result: 32 passed, 0 failed"
   demo reset                              # removes leftovers from earlier runs
   ```
3. **Warm up the Control Center charts.** C3 collects metrics every 60 s, so empty graphs look bad. Around 10 minutes before the start, run `demo load 200`, then `demo reset`. The throughput charts then already show activity.
4. Open **http://localhost:9021** in the browser. Check that the cluster, the 2 connectors and the topics are visible.

### Screen layout
| Window | What | Font |
|---|---|---|
| **A** (left, large) | SSH shell for `demo …` commands | ≥ 16 pt |
| **B** (right) | SSH shell running `demo watch` (Act 7 onward) | ≥ 14 pt |
| **Browser** | Control Center http://localhost:9021 | zoom 125 % |
| *(optional)* | SQL Developer connected through the tunnel (see [Appendix B](#appendix-b--the-same-steps-without-the-helper)) | |

In both SSH windows, run `alias demo=~/cdc-xstream/bin/demo.sh` first.

---

## Act 0: Opening and architecture (5 min)

**Goal:** the customer understands the data path before seeing any commands.

**Say:**
> "Today I'll show a change data capture pipeline from Oracle into Kafka and back out into a database. We use Oracle's own XStream API, which streams changes out of the redo log. There are no triggers on the tables, no polling queries and no extra load on the application. Changes go into Kafka as events with a registered schema, and a JDBC sink connector applies them to a target schema as upserts and deletes. The whole environment runs on one VM and was built by a single `terraform apply`."

**Show this diagram:**

```
 ┌──────────────── Oracle XE 21c (container oracle21c) ─────────────────┐
 │  PDB XEPDB1                                                          │
 │   ORDERMGMT (13 tables)  ── redo/archive log ──►  XStream capture    │
 │        ▲                                          CONFLUENT_XOUT1    │
 │        │ application DML                                │            │
 │        │                                        outbound server XOUT │
 │   ORDERMGMT_SINK (13 tables)  ◄──── MERGE / DELETE ────┐│            │
 └────────────────────────────────────────────────────────┼┼────────────┘
                                                          ││ OCI (XStream Out)
 ┌──────────── Kafka Connect (container connect) ─────────┼┼────────────┐
 │  JDBCSINK0  (JDBC sink, thin driver)  ─────────────────┘│            │
 │     ▲                     XSTREAMCDC0 (XStream source) ◄─┘           │
 └─────┼─────────────────────────────┼──────────────────────────────────┘
       │ consume                     │ produce (Avro)
 ┌─────┴─────────────────────────────▼────────────┐   ┌────────────────────┐
 │ Kafka broker (KRaft)  topics XEPDB1.ORDERMGMT.* │◄─►│ Schema Registry    │
 └─────────────────────────────────────────────────┘   └────────────────────┘
          ▲ metrics (OTLP)
 ┌────────┴─────────┐   ┌──────────────┐
 │ Prometheus       │◄──│ Control      │  ← browser :9021
 │ + Alertmanager   │   │ Center       │
 └──────────────────┘   └──────────────┘
```

**Point out:**
- The **same Oracle database** is used as source (`ORDERMGMT`) and target (`ORDERMGMT_SINK`) to keep the lab small. In production the target would be a different database. The configuration is identical except for the connection URL.
- **Every arrow can pause independently.** Kafka holds the events, retention is infinite in this lab, so either side can stop and catch up later.

**If asked:**
- *Why XStream and not LogMiner?* XStream is Oracle's supported outbound API with push delivery and low latency, and it's the engine behind GoldenGate. The connector attaches as an XStream client. (Licensing: on Enterprise Edition, XStream is covered by an Oracle GoldenGate license. Confirm with Oracle.)
- *Why only one VM?* It's a lab. Production would use 3+ brokers, 2+ Connect workers, and Schema Registry HA (see Act 13).

---

## Act 1: Infrastructure as code (5 min)

**Goal:** show that the environment is reproducible and that database preparation is automated too.

**Do** (laptop, in `oracle_xstr/`):
```bash
ls
ls files/host
terraform state list
terraform output
```

**Expect:**
```
terraform_data.bootstrap        ← upload files, host prep (Docker, swap, disk checks)
terraform_data.oracle           ← XE up, ARCHIVELOG, supplemental logging, XStream users + outbound server
terraform_data.confluent        ← build Connect image, start broker, SR, Connect, C3
terraform_data.connector        ← create XSTREAMCDC0, wait for snapshot, set topic retention
terraform_data.jdbc_sink        ← create sink schema + JDBCSINK0
terraform_data.validation       ← run validate.sh (32 checks, incl. end-to-end insert/delete)
```

**Say:**
> "Terraform drives six stages over SSH. Each stage is an idempotent shell script, and each one re-runs only when its inputs change: a version, a password, the connector config. The Oracle prerequisites that usually take a DBA an afternoon (ARCHIVELOG mode, supplemental logging, GoldenGate replication, the XStream admin and connect users, the capture and outbound server) are all in `files/oracle/`, fully scripted."

**Point out:**
- `variables.tf` lists the knobs: versions (CP 8.3.2, XStream connector 1.6.0, JDBC 10.9.9), captured tables, retention, and toggles for Control Center and the sink.
- Secrets live only in `terraform.tfvars` (git-ignored, mode 600). They're passed to scripts through a mode-600 `.env` and stdin, never on a command line.
- All ports are bound to `127.0.0.1` on the VM. Access goes only through the SSH tunnel.

---

## Act 2: Runtime tour (3 min)

**Goal:** what is running and where.

**Do** (window A):
```bash
demo status
```

**Expect:**
```
NAMES             IMAGE                                                      STATUS
broker            confluentinc/cp-server:8.3.2                               Up
control-center    confluentinc/cp-enterprise-control-center-next-gen:2.5.0   Up
connect           cdc-xstream/connect:8.3.2-xstream-1.6.0-jdbc-10.9.9        Up (healthy)
alertmanager      confluentinc/cp-enterprise-alertmanager:2.5.0              Up
prometheus        confluentinc/cp-enterprise-prometheus:2.5.0                Up
schema-registry   confluentinc/cp-schema-registry:8.3.2                      Up
oracle21c         container-registry.oracle.com/database/express:21.3.0-xe   Up (healthy)

XSTREAMCDC0  source  connector=RUNNING  task=RUNNING
JDBCSINK0    sink    connector=RUNNING  task=RUNNING
```

**Say:**
> "Seven containers under one Docker Compose project. The broker runs in KRaft mode, so there's no ZooKeeper, which is standard from Confluent Platform 8. The Connect image is custom: the stock Connect image plus Oracle Instant Client, because XStream uses the OCI thick driver, plus the XStream and JDBC connector plugins."

---

## Act 3: Oracle source readiness (5 min)

**Goal:** show the database-side prerequisites for CDC. DBAs care about these.

**Do:**
```bash
demo oracle
```

**Expect:**
```
NAME  LOG_MODE    SUPP_MIN  SUPP_ALL  FORCE_LOGGING
XE    ARCHIVELOG  YES       YES       NO
enable_goldengate_replication  TRUE
streams_pool_size              268435456
db_recovery_file_dest_size     21474836480

CAPTURE_NAME     STATUS   CAPTURE_USER  CAPTURED_SCN
CONFLUENT_XOUT1  ENABLED  C##GGADMIN    3992281
SERVER_NAME  CONNECT_USER  CAPTURE_NAME     SOURCE_DATABASE  STATUS
XOUT         C##CFLTUSER   CONFLUENT_XOUT1  XEPDB1           ATTACHED

TABLE_NAME          PRIMARY_KEY
INVENTORIES         PRODUCT_ID, WAREHOUSE_ID
ORDERS              ORDER_ID
ORDER_ITEMS         ORDER_ID, ITEM_ID
...
```

**Say, going top to bottom:**
1. **ARCHIVELOG**: redo is archived, so the capture can read changes even after an online log switches.
2. **Supplemental logging (ALL)**: Oracle writes full before and after row images to redo. That's what gives us complete `before`/`after` in every event.
3. **enable_goldengate_replication = TRUE**: required for XStream.
4. **Capture CONFLUENT_XOUT1**: an Oracle background process mining the redo for the ORDERMGMT tables.
5. **Outbound server XOUT = ATTACHED**: the connector is attached right now. `CAPTURED_SCN` keeps moving forward.
6. **Primary keys**: every captured table has one. The PK becomes the Kafka **message key**, which later drives upserts and deletes in the sink.

**If asked:**
- *Impact on the source?* The capture reads redo; nothing runs against the tables. The main costs are extra redo volume from supplemental logging, plus Streams pool memory.
- *Tables without a PK?* They're still captured, but the key is null. Use `message.key.columns` on the connector, or add a PK or unique key. Otherwise the sink can't do upserts or deletes.
- *How long must archive logs be kept?* Until the capture checkpoint has passed them (`checkpoint_retention_days`, 1 day here). If the connector is down longer than the logs are retained, it has to re-snapshot.

---

## Act 4: Kafka Connect and the XStream source connector (5 min)

**Goal:** what a connector is, and how the source is configured.

**Do:**
```bash
demo plugins
demo config XSTREAMCDC0
```

**Expect (abridged):**
```
sink    io.confluent.connect.jdbc.JdbcSinkConnector                           10.9.9
source  io.confluent.connect.jdbc.JdbcSourceConnector                         10.9.9
source  io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector  1.6.0

{
  "connector.class": "io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector",
  "database.hostname": "oracle21c",
  "database.user": "C##CFLTUSER",
  "database.password": "********",
  "database.dbname": "XE",
  "database.pdb.name": "XEPDB1",
  "database.out.server.name": "XOUT",
  "table.include.list": "ORDERMGMT[.](ORDERS|ORDER_ITEMS|EMPLOYEES|...)",
  "topic.prefix": "XEPDB1",
  "snapshot.mode": "initial",
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  ...
}
```

**Say:**
> "Kafka Connect is the integration runtime. A connector is configuration, not code: a JSON document submitted to a REST API. This source connector logs in as the XStream connect user, attaches to outbound server XOUT, and turns each row change into an event. `snapshot.mode=initial` means it first copied the existing rows, then switched to streaming from the snapshot SCN, with no gap and no overlap. Events are serialised as Avro, and their schemas are registered in Schema Registry."

**Point out:**
- `table.include.list` is a regex: exactly the 13 tables.
- Topic naming is `<topic.prefix>.<SCHEMA>.<TABLE>`, one topic per table.
- Passwords are masked here. In production, use a config provider (file, Vault, AWS Secrets Manager) so no plain-text secrets are stored in Connect.

---

## Act 5: Topics, schemas and a change event (7 min)

**Goal:** understand what lands in Kafka.

**Do:**
```bash
demo topics
demo schema orders
demo event orders 1
```

**Expect:**
```
XEPDB1.ORDERMGMT.ORDERS                  partition=0  messages=105
XEPDB1.ORDERMGMT.ORDER_ITEMS             partition=0  messages=665
... (13 topics)
retention.ms=-1  ... {DYNAMIC_TOPIC_CONFIG:retention.ms=-1, STATIC_BROKER_CONFIG:log.retention.ms=-1, ...}

{"versions":[1]}
{"key_fields":[{"ORDER_ID":"long"}]}
  ORDER_ID     long
  CUSTOMER_ID  int
  STATUS       string
  SALESMAN_ID  ["null","int"]
  ORDER_DATE   {"type":"long",...,"connect.name":"io.debezium.time.Timestamp"}

Offset:104
key:   { "ORDER_ID": 104 }
value:
{
  "before": null,
  "after":  { "ORDER_ID": 104, "CUSTOMER_ID": 18, "STATUS": "Shipped", "SALESMAN_ID": 60, "ORDER_DATE": 1485907200000 },
  "source": { "connector": "Oracle XStream CDC", "db": "XEPDB1", "schema": "ORDERMGMT", "table": "ORDERS",
              "scn": "3682360", "snapshot": "last_in_data_collection", ... },
  "op": "r",
  "ts_ms": 1790502615144
}
```

**Say:**
> "Each table has its own topic. Right after the initial snapshot, message count equals row count: 105 orders, 105 messages. Retention is set to keep forever, both per topic and as the broker default, so Kafka is a full, replayable history of the table.
>
> The schema lives in Schema Registry. Each message carries only a small schema ID, not the whole schema, and every consumer knows the exact structure. The key is the table's primary key.
>
> This is a change event, often called the *Debezium envelope*: `before`, `after`, `op` and `source` metadata. `op=r` means it was read during the snapshot. We'll see `c`, `u` and `d` in a minute."

**Point out:**
- `source.scn`: the Oracle SCN of the change, for ordering and auditing.
- Oracle `DATE` is carried as epoch milliseconds (`io.debezium.time.Timestamp`). `NUMBER(p,s)` columns become Avro decimals; the helper decodes them for display.
- One partition per topic keeps strict order per table in this lab. In production you'd partition by key, which still keeps order per row.

---

## Act 6: Control Center tour (5 min)

**Goal:** the same things, shown in the UI operators use.

**Do** (browser, http://localhost:9021):
1. **Home → the cluster card** (`controlcenter.cluster`). Show brokers (1), topics and partitions, and the throughput charts.
2. **Topics → `XEPDB1.ORDERMGMT.ORDERS`**
   - **Messages** tab: browse the events and open one to show `before`/`after`/`op`.
   - **Schema** tab: the value and key schemas from Schema Registry, with versions.
   - **Configuration** tab: `retention.ms = -1`.
3. **Connect → `connect-default`**: both connectors, `XSTREAMCDC0` (source) and `JDBCSINK0` (sink), both *Running*. Open one to show its settings in a form view.
4. **Consumers** (called *Clients → Consumers* in some versions) **→ `connect-JDBCSINK0`**: the sink's consumer group with **lag per topic**, currently 0. Keep this tab ready for Act 10.

**Say:**
> "Control Center is the operations console: cluster health, topics and their data, schemas, connectors, and consumer lag, which is the key metric for how far behind a target is. It reads broker metrics from Prometheus, and alerting goes through Alertmanager."

**Point out:** everything shown here can also be done through REST APIs and CLIs, which is what the helper script uses. You can automate all of it in CI/CD.

---

## Act 7: Live CDC (INSERT, UPDATE, DELETE) (10 min)

**Goal:** the core of the demo. Changes appear in Kafka and in the target within seconds.

**Do** in window B, and leave it running until Act 12:
```bash
demo watch
```
```
== Watching live change events on: ORDERS ORDER_ITEMS (Ctrl-C to stop)
```
To watch other tables, run for example `demo watch customers products`.

### 7a. INSERT: one transaction, three rows
**Do** (window A):
```bash
demo insert
```
**Expect in A:**
```
SQL> DECLARE ... INSERT INTO orders ... INSERT INTO order_items ... (x2) ... COMMIT;
created ORDER_ID=106 with 2 items (1 transaction, 3 rows)
```
**Expect in B**, about 2 s later:
```
12:54:25 INSERT    ORDERS       scn=3992001  after: {"ORDER_ID":106,"CUSTOMER_ID":1,"STATUS":"Pending","SALESMAN_ID":54,"ORDER_DATE":1790600062000}
12:54:25 INSERT    ORDER_ITEMS  scn=3992001  after: {"ORDER_ID":106,"ITEM_ID":1,"PRODUCT_ID":1,"QUANTITY":2,"UNIT_PRICE":499.99}
12:54:25 INSERT    ORDER_ITEMS  scn=3992001  after: {"ORDER_ID":106,"ITEM_ID":2,"PRODUCT_ID":2,"QUANTITY":1,"UNIT_PRICE":1299}
```
**Say:**
> "One transaction across two tables gives three events on two topics, all with the **same SCN** because they were committed together. Only committed data is streamed; a rolled-back transaction never shows up."

**Then prove it landed in the target:**
```bash
demo compare orders
demo compare order_items
```
```
== ORDERMGMT.ORDERS  WHERE order_id = 106
ORDER_ID  CUSTOMER_ID  STATUS   SALESMAN_ID  ORDER_DATE
106       1            Pending  54           28-SEP-26
== ORDERMGMT_SINK.ORDERS  WHERE order_id = 106
106       1            Pending  54           1790600062000
```
**Point out:** the date column. It's an epoch-ms number in the sink, because the JDBC sink creates columns from the Avro type. A one-line SMT (`TimestampConverter`) or a pre-created target table makes it a real `DATE`/`TIMESTAMP`. Acknowledge this up front rather than let the customer find it.

### 7b. UPDATE: only what changed
```bash
demo update
```
**Expect in B:**
```
12:54:31 UPDATE    ORDERS       scn=3992024  changed: STATUS: Pending -> Shipped
12:54:31 UPDATE    ORDER_ITEMS  scn=3992025  changed: QUANTITY: 2 -> 3
```
```bash
demo compare
```
**Say:**
> "Each update event carries the full before and after image, and the viewer shows just the diff. The target row was updated in place, not duplicated, because the sink upserts by primary key."

To try another status, run `demo update 106 Canceled`.

### 7c. DELETE: including cascades
```bash
demo delete
```
**Expect in B:**
```
12:54:37 DELETE    ORDERS       scn=3992048  before: {"ORDER_ID":106,...,"STATUS":"Shipped",...}
12:54:37  TOMBSTONE key={"ORDER_ID":106}
12:54:37 DELETE    ORDER_ITEMS  scn=3992048  before: {"ORDER_ID":106,"ITEM_ID":1,...}
12:54:37  TOMBSTONE key={"ORDER_ID":106,"ITEM_ID":1}
12:54:37 DELETE    ORDER_ITEMS  scn=3992048  before: {"ORDER_ID":106,"ITEM_ID":2,...}
12:54:37  TOMBSTONE key={"ORDER_ID":106,"ITEM_ID":2}
```
```bash
demo compare orders 106
demo compare order_items 106
```
Both schemas return `(no rows)`.

**Say:**
> "We deleted one order row, and the foreign key's `ON DELETE CASCADE` removed its items. Oracle logs those cascaded deletes in redo too, so they're captured as well. Each delete is followed by a **tombstone**: the key with a null value. The sink turns a tombstone into `DELETE ... WHERE pk = ?`. On a compacted topic, the tombstone would also let Kafka drop the row's history."

---

## Act 8: Inside the JDBC sink (5 min)

**Goal:** how Kafka events become rows.

**Do:**
```bash
demo config JDBCSINK0
demo sinkddl orders
```

**Expect (abridged):**
```
"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
"topics": "XEPDB1.ORDERMGMT.ORDERS,XEPDB1.ORDERMGMT.ORDER_ITEMS,...",
"connection.url": "jdbc:oracle:thin:@//oracle21c:1521/XEPDB1",
"connection.user": "ORDERMGMT_SINK",
"insert.mode": "upsert",
"pk.mode": "record_key",
"delete.enabled": "true",
"auto.create": "true",
"auto.evolve": "true",
"transforms": "unwrap,route",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.unwrap.delete.tombstone.handling.mode": "tombstone",
"transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.route.regex": "XEPDB1\\.ORDERMGMT\\.(.*)",
"transforms.route.replacement": "$1",
"table.name.format": "ORDERMGMT_SINK.${topic}"

CREATE TABLE "ORDERMGMT_SINK"."ORDERS"
 ( "ORDER_ID"    NUMBER(19,0) NOT NULL ENABLE,
   "CUSTOMER_ID" NUMBER(10,0) NOT NULL ENABLE,
   "STATUS"      VARCHAR2(4000) NOT NULL ENABLE,
   "SALESMAN_ID" NUMBER(10,0),
   "ORDER_DATE"  NUMBER(19,0) NOT NULL ENABLE,
   PRIMARY KEY ("ORDER_ID") )
```

**Say, walking through the pipeline:**
```
topic record ──► AvroConverter ──► unwrap ──► route ──► JDBC sink ──► Oracle
 (envelope)       (schema from SR)  (keep "after"; (topic → table  (MERGE on PK,
                                     tombstone      name ORDERS)    DELETE on
                                     passes)                        tombstone)
```
1. **Converter**: bytes become a structured record, using the schema from Schema Registry.
2. **`unwrap` SMT**: flattens the envelope to the plain row (`after`), drops the delete event and keeps its tombstone.
3. **`route` SMT**: renames `XEPDB1.ORDERMGMT.ORDERS` to `ORDERS`, which becomes the table name.
4. **Write**:
   - `upsert` + `record_key` means an Oracle `MERGE ... ON (ORDER_ID)`: insert if missing, update if present.
   - `delete.enabled` turns tombstones into `DELETE`.
   - `auto.create` / `auto.evolve` means the table above was generated from the schema: the PK from the key, nullability from the schema.
5. **Commit**: Kafka offsets are committed only **after** the database commit. That gives at-least-once delivery, and because `MERGE` by PK is idempotent, a replay can't create duplicates.

**If asked:**
- *Is the SMT processing default?* No. The transforms are listed in the connector config and applied to every record the sink reads. The data in Kafka is never modified.
- *Why not `insert` mode?* Every update or replay would fail with `ORA-00001`. `upsert` is the standard for mirroring CDC data.
- *Can I use my own pre-created tables?* Yes. Set `auto.create=false` and create the tables with the proper types (DATE, TIMESTAMP, VARCHAR2(n)).
- *Throughput?* Increase `tasks.max` (tasks split topics and partitions between them), and use `batch.size` plus more partitions per topic.

---

## Act 9: Proof (row counts and latency) (3 min)

**Do:**
```bash
demo counts
demo latency
```

**Expect:**
```
TABLE_NAME          SOURCE_ROWS  SINK_ROWS  STATUS
CONTACTS            319          319        OK
...
ORDERS              105          105        OK
ORDER_ITEMS         665          665        OK
...                 (13 tables, all OK)

== End-to-end latency: Oracle COMMIT -> XStream -> Kafka -> JDBC sink -> Oracle
row visible in ORDERMGMT_SINK.REGIONS after 3118 ms
```

**Say:**
> "All 13 tables match row for row. The latency test commits a row in the source, then polls the target until it appears. About three seconds for the full round trip: redo → capture → Kafka → sink → Oracle commit. On this small single VM, most of that time is batching and poll intervals, and they can be tuned."

---

## Act 10: Burst load and back-pressure (5 min)

**Goal:** show Kafka as a buffer. The source never waits for the target.

**Do:**
```bash
demo pause-sink        # the target is "down for maintenance"
demo load 500          # 500 orders + 500 items, commit every 100
demo lag
demo counts
```

**Expect:**
```
XSTREAMCDC0  source  connector=RUNNING  task=RUNNING
JDBCSINK0    sink    connector=PAUSED   task=PAUSED

500 orders + 500 items committed

TOPIC                                  CURRENT-OFFSET  LOG-END-OFFSET  LAG
XEPDB1.ORDERMGMT.ORDERS                115             615             500
XEPDB1.ORDERMGMT.ORDER_ITEMS           682             1182            500
TOTAL LAG: 1000

ORDERS              605          105        DIFF
ORDER_ITEMS         1165         665        DIFF
```
Window B scrolls with 1,000 INSERT events. The source keeps streaming even though the target is paused.

In **Control Center → Consumers → connect-JDBCSINK0**, the lag is 1,000.

**Then:**
```bash
demo resume-sink
sleep 20; demo lag      # TOTAL LAG: 0
demo counts             # all OK
```

**Say:**
> "The target was unavailable, but the source application and the capture weren't affected at all. Kafka held the changes. When the target came back, it caught up from exactly where it stopped, in order and without gaps. This is the decoupling a point-to-point replication tool doesn't give you: you can add a second, third or tenth consumer (a data warehouse, a search index, a microservice) reading the same topics, each at its own pace."

---

## Act 11: Resilience (Connect outage) (5 min)

**Goal:** the integration layer itself fails, and there's no data loss.

**Do:**
```bash
demo stop-connect      # kills the worker: source AND sink stop
demo insert            # the application keeps working
demo update
demo counts            # ORDERS/ORDER_ITEMS: DIFF (nothing is flowing)
demo start-connect     # ~60 s: worker boots, both connectors resume
sleep 15; demo counts  # all OK again
```

**Expect:**
```
connect stopped
created ORDER_ID=607 with 2 items (1 transaction, 3 rows)
ORDERS              607          606        DIFF
ORDER_ITEMS         1169         1167       DIFF
waiting for connectors..........
XSTREAMCDC0  source  connector=RUNNING  task=RUNNING
JDBCSINK0    sink    connector=RUNNING  task=RUNNING
ORDERS              607          607        OK
ORDER_ITEMS         1169         1169       OK
```
The watcher in window B stays connected to Kafka and prints the missed INSERT and UPDATE as soon as the source resumes.

**Say:**
> "While Connect was down, Oracle kept retaining the changes, because the capture checkpoint didn't move. When the worker restarted, the source resumed from its last stored position and the sink resumed from its last committed offset. Nothing was lost, and nothing was applied twice in the result. In production, Connect runs as a cluster of workers, and tasks fail over to a surviving worker automatically."

---

## Act 12: Schema evolution (5 min)

**Goal:** a DDL change flows through without redeploying anything.

**Do:**
```bash
demo evolve
```
```
SQL> ALTER TABLE orders ADD (priority VARCHAR2(10));
SQL> UPDATE orders SET priority = 'HIGH' WHERE order_id = 607;
SQL> COMMIT;
```
Wait about 10 s, then:
```bash
demo schema orders
demo sinkddl orders
demo compare
```

**Expect:**
```
{"versions":[1,2]}
  ...
  PRIORITY     ["null","string"]

CREATE TABLE "ORDERMGMT_SINK"."ORDERS" ( ... "ORDER_DATE" NUMBER(19,0) NOT NULL ENABLE,
   "PRIORITY" VARCHAR2(4000), PRIMARY KEY ("ORDER_ID") )

== ORDERMGMT.ORDERS  WHERE order_id = 607
ORDER_ID  CUSTOMER_ID  STATUS   SALESMAN_ID  ORDER_DATE  PRIORITY
607       1            Shipped  54           28-SEP-26   HIGH
== ORDERMGMT_SINK.ORDERS  WHERE order_id = 607
607       1            Shipped  54           1790600299000  HIGH
```

**Say:**
> "A DBA added a column. The source connector picked up the DDL from redo, and the next event carried the new field. Schema Registry stored it as **version 2** after checking it's compatible, so existing consumers don't break. The sink saw a field it didn't know and ran `ALTER TABLE ... ADD` on the target by itself (`auto.evolve`)."

**Point out (be honest):** `auto.evolve` only **adds** nullable columns. Drops, renames and type changes are not applied to the target. They need a planned change, which is usually what you want for a production target anyway.

Stop the watcher in window B with `Ctrl-C`.

---

## Act 13: Wrap-up and production considerations (5 min)

**Recap:**
1. Oracle changes are captured from redo via XStream: no triggers, and only committed transactions.
2. They become ordered, schema-governed events in Kafka, with full history and replay.
3. They're applied to a target as idempotent upserts and deletes, within seconds.
4. Each component can stop and catch up without data loss.
5. Everything is built by Terraform and validated automatically (32 checks).

**Going from this lab to production:**

| Area | Lab | Production |
|---|---|---|
| Kafka | 1 broker, RF 1 | ≥ 3 brokers, RF 3, `min.insync.replicas=2`, rack awareness |
| Connect | 1 worker | ≥ 2 workers (distributed); sink `tasks.max` > 1 |
| Schema Registry | 1 node | HA pair; compatibility policy per subject |
| Security | plaintext, localhost + SSH tunnel | TLS + SASL/mTLS, RBAC, secrets via config providers |
| Oracle | XE, same DB for source and target | EE/SE2 source, separate target; GoldenGate licence for XStream; size the Streams pool and archive log retention |
| Data types | DATE → epoch-ms number | pre-created target tables or `TimestampConverter` SMT |
| Retention | infinite | per use case; compacted topics for "latest state" |
| Monitoring | Control Center | C3 + alerts on connector state, consumer lag and capture latency |
| Deployment | Terraform + Docker Compose | Terraform + CFK/Kubernetes, or Confluent Cloud (fully managed XStream connector) |

**Common Q&A:**

| Question | Answer |
|---|---|
| Does the source table get locked during the snapshot? | Only a brief shared lock while the table structure is captured (it blocks DDL, not DML). The rows are read with a flashback query `AS OF SCN`, so the application keeps writing. |
| What if the connector is down for days? | It resumes if the archive logs from that period still exist. Otherwise it re-snapshots. Size the retention accordingly. |
| Exactly-once? | The source delivers at-least-once. The sink's PK upsert makes the result idempotent, so the end state is exact. |
| Ordering? | Guaranteed per key (partition). One transaction's rows share an SCN. |
| Multiple targets? | Yes. Add more sink connectors or consumers on the same topics; they're independent. |
| LOBs, XMLType? | Supported with limits by the XStream connector (check the docs for the version). Test them explicitly. |
| Tables without PK? | They're captured with a null key. Use `message.key.columns`, or add a unique key. |
| Initial load of large tables? | Snapshot mode `initial` handles it. For very large tables, consider incremental or ad-hoc snapshots via the signal table. |

---

## After the demo

```bash
demo reset
```
This does the following:
- deletes every order created during the demo (IDs above the baseline recorded in `~/cdc-xstream/.demo_baseline`; ORDER_ITEMS cascade)
- drops the `PRIORITY` column from the source and from the sink, where it has to be dropped explicitly because the sink never drops columns
- makes sure Connect is running and the sink is resumed
- waits, then prints `demo counts`, which should be all OK with ORDERS = 105 and ORDER_ITEMS = 665

Optionally, run `bash ~/cdc-xstream/bin/validate.sh` and expect `32 passed, 0 failed`.

The Schema Registry keeps version 2 of the ORDERS schema (with PRIORITY). That's expected, because the registry is an append-only history. The next change to ORDERS registers the schema without PRIORITY as a new version.

---

## If something goes wrong

| Symptom | Quick fix |
|---|---|
| `demo watch` shows nothing | Check `demo status`. If the source task isn't RUNNING, run `curl -s -X POST 'localhost:8083/connectors/XSTREAMCDC0/restart?includeTasks=true'` |
| A connector is `FAILED` | `demo config <NAME>` to check the config, then `curl -s localhost:8083/connectors/<NAME>/status \| jq -r '.tasks[0].trace' \| head -20`, and restart as above |
| `compare` shows the sink behind | Wait 5–10 s, or run `demo lag`. If the sink is paused, run `demo resume-sink` |
| Control Center blank or slow | Hard refresh. Charts need 1–2 min after a restart. Check `docker logs --tail 50 control-center` |
| Browser can't reach :9021 | The SSH tunnel has dropped. Restart it (see [Before the demo](#before-the-demo)) |
| `start-connect` takes over 2 min | `docker logs --tail 80 connect`. The worker loads 3 plugin sets at boot; 60–90 s is normal |
| SSH fails | The public IP may have changed after an instance restart. Check the EC2 console, then update `host` in `terraform.tfvars` |
| Anything else | `bash ~/cdc-xstream/bin/validate.sh` pinpoints the failing layer |

---

## Appendix A: helper command reference

`demo` = `~/cdc-xstream/bin/demo.sh`, run on the host.

| Command | What it does |
|---|---|
| `status` | containers and connector states |
| `oracle` | ARCHIVELOG, supplemental logging, XStream capture/outbound, captured tables and PKs |
| `plugins` | connector plugins in the Connect worker |
| `config [NAME]` | connector config, passwords masked (default XSTREAMCDC0) |
| `topics` | CDC topics with message counts, retention |
| `schema [TABLE]` | Schema Registry versions, key fields and row fields |
| `event [TABLE] [N]` | last N events of a topic, pretty-printed (decimals decoded) |
| `sinkddl [TABLE]` | DDL of the auto-created sink table |
| `watch [TABLE...]` | live one-line view of change events (default ORDERS ORDER_ITEMS) |
| `insert` | new order + 2 items in one transaction |
| `update [ID] [STATUS]` | status change + quantity change (default: last demo order, Shipped) |
| `delete [ID]` | delete order; items cascade |
| `compare [TABLE] [ID\|WHERE]` | the same rows from source and sink |
| `counts` | row counts, source vs sink, all tables |
| `lag` | sink consumer-group lag per topic |
| `latency` | commit-to-sink latency in ms |
| `load [N]` | burst of N orders (+N items), commit every 100 |
| `pause-sink` / `resume-sink` | pause or resume JDBCSINK0 |
| `stop-connect` / `start-connect` | stop or start the Connect worker (waits until RUNNING) |
| `evolve` | `ALTER TABLE orders ADD priority` + update |
| `reset` | remove all demo changes, verify counts |

---

## Appendix B: the same steps without the helper

For customers who want to see the native tooling. Run these on the host.

```bash
# SQL*Plus as the application user (source)
docker exec -it oracle21c sqlplus ordermgmt/kafka@//localhost:1521/XEPDB1

# SQL*Plus as SYSDBA in the PDB (can read both schemas)
docker exec -it -e ORACLE_SID=XE oracle21c sqlplus / as sysdba
SQL> ALTER SESSION SET CONTAINER = XEPDB1;
SQL> SELECT * FROM ordermgmt_sink.orders WHERE order_id = 106;

# Connect REST API
curl -s localhost:8083/connectors?expand=status | jq
curl -s localhost:8083/connectors/JDBCSINK0/config | jq
curl -s -X PUT localhost:8083/connectors/JDBCSINK0/pause
curl -s -X PUT localhost:8083/connectors/JDBCSINK0/resume

# Topics and consumer lag
docker exec broker kafka-topics --bootstrap-server broker:29092 --list
docker exec broker kafka-consumer-groups --bootstrap-server broker:29092 --describe --group connect-JDBCSINK0

# Read the raw Avro events (Ctrl-C to stop)
docker exec -it schema-registry kafka-avro-console-consumer --bootstrap-server broker:29092 \
  --topic XEPDB1.ORDERMGMT.ORDERS --from-beginning \
  --formatter-property schema.registry.url=http://schema-registry:8081 --formatter-property print.key=true

# Schema Registry
curl -s localhost:8081/subjects | jq
curl -s localhost:8081/subjects/XEPDB1.ORDERMGMT.ORDERS-value/versions/latest | jq -r .schema | jq
```

**SQL Developer / DBeaver from your laptop** (through the SSH tunnel):

| Connection | Host | Port | Service name | User | Password |
|---|---|---|---|---|---|
| Source | localhost | 1521 | XEPDB1 | ORDERMGMT | `kafka` |
| Sink | localhost | 1521 | XEPDB1 | ORDERMGMT_SINK | `sink_schema_password` from `terraform.tfvars` |

With both schemas side by side in a GUI, Act 7 is very visual: run `demo insert`/`update`/`delete` and refresh both grids.
