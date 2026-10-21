############################################
# Rendered configuration
############################################

locals {
  oracle_cdb = "XE"     # fixed by the XE image
  oracle_pdb = "XEPDB1" # fixed by the XE image

  # With ports bound to 127.0.0.1, external Kafka clients come through an SSH
  # tunnel, so the broker must advertise localhost.
  kafka_advertised_host = var.bind_address == "127.0.0.1" ? "localhost" : var.host

  # Single source of truth on the host: read by docker compose and by every bin/*.sh script.
  env_file = templatefile("${path.module}/templates/env.tftpl", {
    cp_version                = var.cp_version
    oracle_image              = var.oracle_image
    xstream_connector_version = var.xstream_connector_version
    jdbc_connector_version    = var.jdbc_connector_version
    c3_version                = var.control_center_version
    c3_enabled                = var.enable_control_center
    c3_heap                   = var.control_center_heap
    jdbc_sink_enabled         = var.enable_jdbc_sink
    jdbc_sink_name            = var.jdbc_sink_connector_name
    sink_schema               = upper(var.sink_schema)
    sink_schema_pwd           = var.sink_schema_password
    instant_client_url        = var.instant_client_url
    docker_compose_version    = var.docker_compose_version
    docker_buildx_version     = var.docker_buildx_version
    sample_data_base_url      = "https://raw.githubusercontent.com/ora0600/confluent-new-cdc-connector/${var.sample_data_ref}/oraclexe21c/docker/scripts"
    bind_address              = var.bind_address
    kafka_advertised_host     = local.kafka_advertised_host
    kafka_cluster_id          = var.kafka_cluster_id
    kafka_heap                = var.kafka_heap
    schema_registry_heap      = var.schema_registry_heap
    connect_heap              = var.connect_heap
    oracle_pwd                = var.oracle_sys_password
    oracle_pdb                = local.oracle_pdb
    admin_user                = upper(var.xstream_admin_user)
    admin_pwd                 = var.xstream_admin_password
    cdc_user                  = upper(var.xstream_connect_user)
    cdc_pwd                   = var.xstream_connect_password
    outbound_server           = upper(var.outbound_server_name)
    capture_name              = upper(var.capture_name)
    capture_schema            = upper(var.capture_schema)
    redo_log_size_mb          = var.redo_log_size_mb
    recovery_area_size_gb     = var.recovery_area_size_gb
    min_free_disk_gb          = var.min_free_disk_gb
    swap_size_gb              = var.swap_size_gb
    connector_name            = var.connector_name
    topic_prefix              = var.topic_prefix
    topic_retention_ms        = var.topic_retention_ms
  })

  create_outbound_sql = templatefile("${path.module}/templates/03_create_outbound.sql.tftpl", {
    outbound_server = upper(var.outbound_server_name)
    capture_name    = upper(var.capture_name)
    pdb             = local.oracle_pdb
    schema          = upper(var.capture_schema)
    tables          = [for t in var.capture_tables : upper(t)]
    cdc_user        = upper(var.xstream_connect_user)
    retention_days  = var.checkpoint_retention_days
  })

  # Body for PUT /connectors/<name>/config
  connector_config = jsonencode(merge({
    "connector.class"          = "io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector"
    "tasks.max"                = "1"
    "database.hostname"        = "oracle21c"
    "database.port"            = "1521"
    "database.user"            = upper(var.xstream_connect_user)
    "database.password"        = var.xstream_connect_password
    "database.dbname"          = local.oracle_cdb
    "database.service.name"    = local.oracle_cdb
    "database.pdb.name"        = local.oracle_pdb
    "database.out.server.name" = upper(var.outbound_server_name)
    "table.include.list"       = "${upper(var.capture_schema)}[.](${join("|", [for t in var.capture_tables : upper(t)])})"
    "topic.prefix"             = var.topic_prefix
    "snapshot.mode"            = "initial"

    "schema.history.internal.kafka.bootstrap.servers" = "broker:29092"
    "schema.history.internal.kafka.topic"             = "__orcl-schema-changes.${var.topic_prefix}"

    # Single broker: every internal/auto-created topic needs RF 1
    "confluent.topic.bootstrap.servers"         = "broker:29092"
    "confluent.topic.replication.factor"        = "1"
    "topic.creation.default.replication.factor" = "1"
    "topic.creation.default.partitions"         = "1"
    "topic.creation.default.retention.ms"       = tostring(var.topic_retention_ms)

    "key.converter"                       = "io.confluent.connect.avro.AvroConverter"
    "key.converter.schema.registry.url"   = "http://schema-registry:8081"
    "value.converter"                     = "io.confluent.connect.avro.AvroConverter"
    "value.converter.schema.registry.url" = "http://schema-registry:8081"
  }, var.confluent_license == "" ? {} : { "confluent.license" = var.confluent_license }))

  # Body for PUT /connectors/<name>/config of the JDBC sink.
  # Flattens the CDC envelope to the row image, maps topic -> table name and
  # upserts/deletes by primary key (the record key) into sink_schema.
  jdbc_sink_config = jsonencode({
    "connector.class"     = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"           = "1"
    "topics"              = join(",", [for t in var.capture_tables : "${var.topic_prefix}.${upper(var.capture_schema)}.${upper(t)}"])
    "connection.url"      = "jdbc:oracle:thin:@//oracle21c:1521/${local.oracle_pdb}"
    "connection.user"     = upper(var.sink_schema)
    "connection.password" = var.sink_schema_password
    "dialect.name"        = "OracleDatabaseDialect"

    "insert.mode"           = "upsert"
    "pk.mode"               = "record_key"
    "delete.enabled"        = "true"
    "auto.create"           = "true"
    "auto.evolve"           = "true"
    "quote.sql.identifiers" = "always"
    # $${topic} is the JDBC sink placeholder (escaped from Terraform), after the route SMT: <TABLE>
    "table.name.format" = "${upper(var.sink_schema)}.$${topic}"

    "transforms" = "unwrap,route"
    # Delete = delete event + tombstone from the source: drop the former, keep
    # the tombstone so the sink issues DELETE ... WHERE <pk>.
    "transforms.unwrap.type"                           = "io.debezium.transforms.ExtractNewRecordState"
    "transforms.unwrap.delete.tombstone.handling.mode" = "tombstone"
    "transforms.route.type"                            = "org.apache.kafka.connect.transforms.RegexRouter"
    "transforms.route.regex"                           = "${replace(var.topic_prefix, ".", "\\.")}\\.${upper(var.capture_schema)}\\.(.*)"
    "transforms.route.replacement"                     = "$1"

    "key.converter"                       = "io.confluent.connect.avro.AvroConverter"
    "key.converter.schema.registry.url"   = "http://schema-registry:8081"
    "value.converter"                     = "io.confluent.connect.avro.AvroConverter"
    "value.converter.schema.registry.url" = "http://schema-registry:8081"
  })

  compose_file = file("${path.module}/templates/docker-compose.yml")

  # Change detection per stage: a stage re-runs only when its inputs change.
  host_files_hash    = sha256(join("", [for f in sort(fileset("${path.module}/files/host", "*")) : filesha256("${path.module}/files/host/${f}")]))
  oracle_files_hash  = sha256(join("", [for f in sort(fileset("${path.module}/files/oracle", "*")) : filesha256("${path.module}/files/oracle/${f}")]))
  connect_files_hash = sha256(join("", [for f in sort(fileset("${path.module}/files/connect", "*")) : filesha256("${path.module}/files/connect/${f}")]))
  c3_files_hash      = sha256(join("", [for f in sort(fileset("${path.module}/files/c3", "*")) : filesha256("${path.module}/files/c3/${f}")]))
}

check "jdbc_sink_password" {
  assert {
    condition     = !var.enable_jdbc_sink || var.sink_schema_password != ""
    error_message = "enable_jdbc_sink = true needs sink_schema_password in terraform.tfvars."
  }
}

############################################
# Stage 0 - teardown hook
# Created first / destroyed last. Its only job is to stop the stack on
# 'terraform destroy'. Destroy-time provisioners may only use self.
############################################

resource "terraform_data" "teardown" {
  input = {
    host     = var.host
    user     = var.ssh_user
    key_path = var.ssh_private_key_path
    dir      = var.remote_dir
    purge    = var.destroy_purges_data
  }

  connection {
    type        = "ssh"
    host        = self.input.host
    user        = self.input.user
    private_key = file(self.input.key_path)
  }

  provisioner "remote-exec" {
    when = destroy
    inline = [
      "if [ -f ${self.input.dir}/docker-compose.yml ]; then cd ${self.input.dir} && sudo docker compose down --remove-orphans ${self.input.purge ? "--volumes" : ""} || true; fi",
      self.input.purge ? "sudo rm -rf ${self.input.dir}" : "echo 'Data volumes kept (destroy_purges_data = false)'",
    ]
  }
}

############################################
# Stage 1 - bootstrap
# Upload all files, run preflight checks (OS, CPU arch, disk, RAM) and
# prepare the host (Docker, Compose, Buildx, kernel/ulimit settings, swap).
############################################

resource "terraform_data" "bootstrap" {
  depends_on = [terraform_data.teardown]

  triggers_replace = [
    var.host,
    local.host_files_hash,
    local.oracle_files_hash,
    local.connect_files_hash,
    local.c3_files_hash,
    sha256(local.env_file),
    sha256(local.compose_file),
    sha256(local.create_outbound_sql),
    sha256(local.connector_config),
    sha256(local.jdbc_sink_config),
  ]

  connection {
    type        = "ssh"
    host        = var.host
    user        = var.ssh_user
    private_key = file(var.ssh_private_key_path)
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = ["mkdir -p ${var.remote_dir}/bin ${var.remote_dir}/oracle ${var.remote_dir}/connect ${var.remote_dir}/connector ${var.remote_dir}/c3 /tmp/cdc-xstream-c3"]
  }

  provisioner "file" {
    source      = "${path.module}/files/host/"
    destination = "${var.remote_dir}/bin"
  }

  provisioner "file" {
    source      = "${path.module}/files/oracle/"
    destination = "${var.remote_dir}/oracle"
  }

  provisioner "file" {
    source      = "${path.module}/files/connect/"
    destination = "${var.remote_dir}/connect"
  }

  provisioner "file" {
    content     = local.create_outbound_sql
    destination = "${var.remote_dir}/oracle/03_create_outbound.sql"
  }

  provisioner "file" {
    content     = local.compose_file
    destination = "${var.remote_dir}/docker-compose.yml"
  }

  provisioner "file" {
    content     = local.env_file
    destination = "${var.remote_dir}/.env"
  }

  provisioner "file" {
    content     = local.connector_config
    destination = "${var.remote_dir}/connector/xstream-cdc.json"
  }

  provisioner "file" {
    content     = local.jdbc_sink_config
    destination = "${var.remote_dir}/connector/jdbc-sink.json"
  }

  # Seed configs only: Control Center rewrites alertmanager-/trigger_rules-generated.yml
  # at runtime, so an existing file on the host is never overwritten.
  provisioner "file" {
    source      = "${path.module}/files/c3/"
    destination = "/tmp/cdc-xstream-c3"
  }

  provisioner "remote-exec" {
    inline = [
      "for f in /tmp/cdc-xstream-c3/*; do [ -e ${var.remote_dir}/c3/$(basename $f) ] || cp $f ${var.remote_dir}/c3/; done; rm -rf /tmp/cdc-xstream-c3",
      "chmod 600 ${var.remote_dir}/.env ${var.remote_dir}/connector/xstream-cdc.json ${var.remote_dir}/connector/jdbc-sink.json",
      "chmod +x ${var.remote_dir}/bin/*.sh",
      "sudo bash ${var.remote_dir}/bin/00_preflight.sh",
      "sudo bash ${var.remote_dir}/bin/01_host_prep.sh",
    ]
  }
}

############################################
# Stage 2 - Oracle XE 21c
# Start the container, wait for DB creation, then enable XStream:
# goldengate replication, ARCHIVELOG, supplemental logging, sample schema,
# XStream users and the outbound server.
############################################

resource "terraform_data" "oracle" {
  depends_on = [terraform_data.bootstrap]

  triggers_replace = [
    var.host,
    var.oracle_image,
    local.oracle_files_hash,
    sha256(local.create_outbound_sql),
  ]

  connection {
    type        = "ssh"
    host        = var.host
    user        = var.ssh_user
    private_key = file(var.ssh_private_key_path)
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = ["sudo bash ${var.remote_dir}/bin/02_deploy_oracle.sh"]
  }
}

############################################
# Stage 3 - Confluent Platform 8.x
# KRaft broker + Schema Registry + a Connect worker image that bundles the
# XStream connector and Oracle Instant Client.
############################################

resource "terraform_data" "confluent" {
  depends_on = [terraform_data.oracle]

  triggers_replace = [
    var.host,
    var.cp_version,
    var.xstream_connector_version,
    var.instant_client_url,
    var.jdbc_connector_version,
    var.enable_control_center,
    var.control_center_version,
    var.control_center_heap,
    local.connect_files_hash,
    local.c3_files_hash,
    sha256(local.compose_file),
  ]

  connection {
    type        = "ssh"
    host        = var.host
    user        = var.ssh_user
    private_key = file(var.ssh_private_key_path)
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = ["sudo bash ${var.remote_dir}/bin/03_deploy_confluent.sh"]
  }
}

############################################
# Stage 4 - XStream CDC source connector
############################################

resource "terraform_data" "connector" {
  depends_on = [terraform_data.confluent]

  triggers_replace = [
    var.host,
    sha256(local.connector_config),
    terraform_data.oracle.id,
    terraform_data.confluent.id,
  ]

  connection {
    type        = "ssh"
    host        = var.host
    user        = var.ssh_user
    private_key = file(var.ssh_private_key_path)
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = ["sudo bash ${var.remote_dir}/bin/04_deploy_connector.sh"]
  }
}

############################################
# Stage 5 - JDBC sink connector (CDC topics -> Oracle sink_schema)
# Creates the sink schema, then the connector. Disabling it deletes the
# connector but keeps the schema and its tables.
############################################

resource "terraform_data" "jdbc_sink" {
  depends_on = [terraform_data.connector]

  triggers_replace = [
    var.host,
    var.enable_jdbc_sink,
    sha256(local.jdbc_sink_config),
    terraform_data.connector.id,
  ]

  connection {
    type        = "ssh"
    host        = var.host
    user        = var.ssh_user
    private_key = file(var.ssh_private_key_path)
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = ["sudo bash ${var.remote_dir}/bin/05_deploy_jdbc_sink.sh"]
  }
}

############################################
# Stage 6 - validation
# Re-run any time with: terraform apply -replace='terraform_data.validation[0]'
############################################

resource "terraform_data" "validation" {
  count      = var.run_validation ? 1 : 0
  depends_on = [terraform_data.jdbc_sink]

  triggers_replace = [terraform_data.connector.id, terraform_data.jdbc_sink.id]

  connection {
    type        = "ssh"
    host        = var.host
    user        = var.ssh_user
    private_key = file(var.ssh_private_key_path)
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = ["sudo bash ${var.remote_dir}/bin/validate.sh"]
  }
}
