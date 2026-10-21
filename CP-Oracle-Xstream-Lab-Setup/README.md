# Oracle XE 21c → XStream CDC → Confluent Platform 8.x → Oracle (Terraform lab)

Terraform turns **one existing EC2 host** into a complete change-data-capture lab, all in Docker:

- **Oracle Database 21c XE** with XStream enabled and the `ORDERMGMT` sample schema
- **Confluent Platform 8.3** (KRaft broker, Schema Registry, Kafka Connect, Control Center)
- **Oracle XStream CDC Source connector** `XSTREAMCDC0`: Oracle `ORDERMGMT` → Kafka topics (Avro)
- **JDBC Sink connector** `JDBCSINK0`: Kafka topics → Oracle `ORDERMGMT_SINK` (inserts, updates, deletes)

```
 Oracle XEPDB1.ORDERMGMT ──XStream (XOUT)──▶ XSTREAMCDC0 ──▶ Kafka topics XEPDB1.ORDERMGMT.<TABLE> ──▶ JDBCSINK0 ──▶ Oracle XEPDB1.ORDERMGMT_SINK
                                                              (Avro, schemas in Schema Registry)
```

| Container | Purpose | Port (on host `127.0.0.1`) |
|-----------|---------|------|
| `oracle21c` | Oracle XE 21c (CDB `XE`, PDB `XEPDB1`) | 1521 |
| `broker` | Kafka (cp-server, KRaft) | 9092 (inside Docker: `broker:29092`) |
| `schema-registry` | Schema Registry | 8081 |
| `connect` | Kafka Connect with the XStream CDC and JDBC connectors | 8083 |
| `control-center`, `prometheus`, `alertmanager` | Control Center UI | 9021 |

For a customer-facing walkthrough with talk track, see [DEMO.md](DEMO.md).

---

## Part 1: Deploy

### Prerequisites

- Terraform ≥ 1.5 on your laptop
- An **x86_64 Amazon Linux 2023** EC2 host, **≥ 16 GB RAM** (`t3.xlarge`), **100 GB root volume**
- Security group: TCP 22 from your IP. The host needs outbound HTTPS.

### Step 1: Configure

```bash
cd CP-Oracle-Xstream-Lab-Setup
cp terraform.tfvars.example terraform.tfvars
vi terraform.tfvars      # set host, ssh_private_key_path and the four passwords
```

### Step 2: Apply

```bash
terraform init
terraform apply          # ~30–45 min on the first run
```

The run ends with `validate.sh` printing PASS/FAIL for every check, including an end-to-end
insert/delete through the whole pipeline. If everything passes, the lab is ready.

---

## Part 2: Explore the lab (step by step)

Unless stated otherwise, run commands **on the EC2 host** from the lab directory:

```bash
cd ~/cdc-xstream
```

> `bin/demo.sh` is a helper that wraps the longer commands. Each section shows the helper
> and the plain command it runs.

### Step 3: See the Terraform resources and outputs (laptop)

```bash
terraform state list          # the terraform_data stages that were applied
terraform output              # every output
terraform output kafka_topics # the CDC topics
```

Log in and open the tunnel (keep the tunnel running in a separate terminal):

```bash
$(terraform output -raw ssh)           # SSH into the host
$(terraform output -raw ssh_tunnel)    # forwards 1521, 9092, 8081, 8083, 9021 to your laptop
```

### Step 4: See what was created on the host

```bash
ls -la ~/cdc-xstream                          # .env, docker-compose.yml, bin/, oracle/, connect/, connector/
sudo docker compose ps                        # all containers and their state
sudo docker images | grep -E 'cdc-xstream|confluentinc|oracle'
sudo docker volume ls | grep cdc-xstream      # persistent Oracle and Kafka data
bin/demo.sh status                            # containers + connector states in one view
```

### Step 5: Oracle source and XStream

```bash
bin/demo.sh oracle
```

This shows ARCHIVELOG, supplemental logging, the capture process `CONFLUENT_XOUT1`, the
outbound server `XOUT` and the captured tables with their primary keys.

To query it yourself:

```bash
sudo docker exec -it -e ORACLE_SID=XE oracle21c sqlplus / as sysdba
```
```sql
SELECT log_mode, supplemental_log_data_min, supplemental_log_data_all FROM v$database;
SELECT capture_name, status FROM dba_capture;
SELECT server_name, connect_user, status FROM dba_xstream_outbound;
SELECT server_name, state, total_messages_sent FROM gv$xstream_outbound_server;
ALTER SESSION SET CONTAINER = XEPDB1;
SELECT table_name, num_rows FROM dba_tables WHERE owner = 'ORDERMGMT' ORDER BY 1;
EXIT
```

### Step 6: Confluent Platform components

```bash
# Kafka broker (KRaft)
sudo docker exec broker kafka-metadata-quorum --bootstrap-server broker:29092 describe --status
sudo docker exec broker kafka-broker-api-versions --bootstrap-server broker:29092 | head -1

# Schema Registry
curl -s localhost:8081/config | jq            # global compatibility level
curl -s localhost:8081/subjects | jq          # all subjects

# Kafka Connect
curl -s localhost:8083/ | jq                                # worker version
bin/demo.sh plugins                                         # installed plugins (XStream CDC + JDBC)
curl -s "localhost:8083/connectors?expand=status" | jq      # both connectors with status
```

**Control Center:** with the tunnel open, browse to **http://localhost:9021**. Under **Topics**
you'll find the topics and their messages, under **Connect** both connectors, and under
**Schema Registry** the subjects. Broker charts fill in after a couple of minutes.

### Step 7: Source connector `XSTREAMCDC0`

```bash
curl -s localhost:8083/connectors/XSTREAMCDC0/status | jq   # connector + task RUNNING
bin/demo.sh config XSTREAMCDC0                              # config, passwords masked
curl -s localhost:8083/connectors/XSTREAMCDC0/topics | jq   # topics it writes to
sudo docker compose logs --tail 50 connect                  # worker log
```

### Step 8: Kafka topics

```bash
# List the CDC topics
sudo docker exec broker kafka-topics --bootstrap-server broker:29092 --list | grep XEPDB1

# Partitions, leader and config of one topic
sudo docker exec broker kafka-topics --bootstrap-server broker:29092 --describe --topic XEPDB1.ORDERMGMT.ORDERS

# Message count per topic and retention
bin/demo.sh topics

# Read the first 3 events (Avro, decoded with Schema Registry)
sudo docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker:29092 --topic XEPDB1.ORDERMGMT.REGIONS --from-beginning --max-messages 3 \
  --formatter-property schema.registry.url=http://schema-registry:8081 \
  --formatter-property print.key=true

# Last event of a topic, pretty-printed (op, before, after, source.scn)
bin/demo.sh event ORDERS
```

Each event has `op` (`r` = snapshot, `c` = insert, `u` = update, `d` = delete), `before`,
`after` and `source` (SCN, table, transaction). The message key is the table's primary key.

### Step 9: Schema Registry subjects

Each topic has a `-key` and a `-value` subject.

```bash
curl -s localhost:8081/subjects | jq
curl -s localhost:8081/subjects/XEPDB1.ORDERMGMT.ORDERS-value/versions | jq
curl -s localhost:8081/subjects/XEPDB1.ORDERMGMT.ORDERS-value/versions/latest | jq -r .schema | jq
curl -s localhost:8081/subjects/XEPDB1.ORDERMGMT.ORDERS-key/versions/latest   | jq -r .schema | jq

bin/demo.sh schema ORDERS     # summary: versions, key fields, value columns
```

### Step 10: Sink connector `JDBCSINK0` and the sink tables

```bash
curl -s localhost:8083/connectors/JDBCSINK0/status | jq   # connector + task RUNNING
bin/demo.sh config JDBCSINK0                              # config, passwords masked

# Consumer lag of the sink (0 = fully caught up)
sudo docker exec broker kafka-consumer-groups --bootstrap-server broker:29092 --describe --group connect-JDBCSINK0

bin/demo.sh counts            # row counts ORDERMGMT vs ORDERMGMT_SINK, every table should be OK
bin/demo.sh sinkddl ORDERS    # the table DDL the sink generated (auto.create)
```

The sink creates one table per topic in `ORDERMGMT_SINK` (`XEPDB1.ORDERMGMT.ORDERS` →
`ORDERMGMT_SINK.ORDERS`). Oracle `DATE` columns arrive as epoch milliseconds and are stored as `NUMBER(19)`.

---

## Part 3: End-to-end INSERT / UPDATE / DELETE

Open **two SSH sessions** on the host, both in `~/cdc-xstream`.

**Terminal 1: watch the change events live** (Ctrl-C to stop)

```bash
bin/demo.sh watch ORDERS ORDER_ITEMS
```

**Terminal 2: change the source and compare with the sink**

```bash
# 1. INSERT: one order with 2 items, in one transaction
bin/demo.sh insert
bin/demo.sh compare ORDERS          # same row in ORDERMGMT and ORDERMGMT_SINK
bin/demo.sh compare ORDER_ITEMS

# 2. UPDATE: status -> Shipped, quantity + 1
bin/demo.sh update
bin/demo.sh compare ORDERS

# 3. DELETE: the order (its items go too, via ON DELETE CASCADE)
bin/demo.sh delete
bin/demo.sh compare ORDERS          # gone from both schemas

# 4. Proof
bin/demo.sh counts                  # all tables OK
bin/demo.sh latency                 # time from Oracle COMMIT until the row is in the sink
```

Terminal 1 shows one line per event: `INSERT` with the new row, `UPDATE` with only the
changed columns (`before -> after`), `DELETE` followed by a `TOMBSTONE` (which makes the sink
delete the row).

### The same flow by hand in SQL*Plus

```bash
sudo docker exec -it oracle21c sqlplus ordermgmt/kafka@//localhost:1521/XEPDB1
```
```sql
INSERT INTO regions (region_name) VALUES ('LAB_TEST');
COMMIT;
UPDATE regions SET region_name = 'LAB_TEST_UPDATED' WHERE region_name = 'LAB_TEST';
COMMIT;
DELETE FROM regions WHERE region_name = 'LAB_TEST_UPDATED';
COMMIT;
EXIT
```

After each `COMMIT`, check Kafka and the sink from another terminal:

```bash
bin/demo.sh event REGIONS 2        # the last 2 events on XEPDB1.ORDERMGMT.REGIONS

sudo docker exec -i -e ORACLE_SID=XE oracle21c sqlplus -s / as sysdba <<'EOF'
ALTER SESSION SET CONTAINER = XEPDB1;
SELECT * FROM ordermgmt_sink.regions WHERE region_name LIKE 'LAB_TEST%';
EOF
```

You should see the row appear (insert), change name (update), then disappear (delete).

### Clean up the demo data

```bash
bin/demo.sh reset                  # deletes the orders the demo added, then shows counts
```

Run `bin/demo.sh` with no arguments to list all helper commands (load test, pause sink,
Connect outage, schema evolution).

---

## Part 4: Operate

| Task | How |
|------|-----|
| Re-run all checks | laptop: `terraform output -raw validate \| sh` |
| Change settings / tables / versions | edit `terraform.tfvars` → `terraform apply` |
| Turn Control Center or the JDBC sink on/off | `enable_control_center` / `enable_jdbc_sink` → `terraform apply` |
| Restart everything | host: `sudo docker compose restart` (data is on volumes) |
| Free archive-log space | host: `sudo bash bin/purge_archivelogs.sh 1` |
| Host IP changed after stop/start | update `host` in `terraform.tfvars` |

### Tear down

```bash
terraform destroy
```

This removes the containers and keeps the Oracle and Kafka volumes. Set
`destroy_purges_data = true` and run `terraform apply` once before destroying to delete the data too.

### Troubleshooting

| Symptom | Fix |
|---------|-----|
| `PREFLIGHT FAILED: not enough disk` | enlarge the root EBS volume to 100 GB and re-apply (the filesystem grows automatically) |
| Oracle stage takes long | first DB creation takes 10–25 min. Watch `sudo docker logs -f oracle21c` |
| Connector `FAILED` | `curl -s localhost:8083/connectors/XSTREAMCDC0/status \| jq -r '.tasks[0].trace'` and `sudo docker compose logs connect` |
| Connector RUNNING but no new events | as SYSDBA: `SELECT state FROM v$xstream_capture;` (capture may be waiting for redo) |
| Sink counts show `DIFF` | check lag (Step 10). It drains within seconds unless the sink task failed |
| `ORA-00257` (archiver stuck) | `sudo bash bin/purge_archivelogs.sh 1` |

## Security notes (lab only)

- Passwords are in `terraform.tfvars` (git-ignored), Terraform state and the host's `.env` (mode 600).
- Kafka, Schema Registry and Connect run without TLS or auth and listen on `127.0.0.1` only. Use the SSH tunnel.
- `C##GGADMIN` has `DBA` and the sample user is `ordermgmt/kafka`. Don't reuse this setup beyond a lab.
