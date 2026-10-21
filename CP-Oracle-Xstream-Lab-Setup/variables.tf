############################################
# Target host / SSH
############################################

variable "host" {
  description = "Public IP or DNS name of the existing EC2 host that will run the whole stack."
  type        = string
}

variable "ssh_user" {
  description = "SSH user on the host (Amazon Linux 2023 = ec2-user)."
  type        = string
  default     = "ec2-user"
}

variable "ssh_private_key_path" {
  description = "Path on YOUR machine to the private key (.pem) used to SSH into the host."
  type        = string
}

variable "remote_dir" {
  description = "Directory on the host where compose files, scripts and config are placed."
  type        = string
  default     = "/home/ec2-user/cdc-xstream"
}

variable "min_free_disk_gb" {
  description = "Preflight fails if the root filesystem has less free space than this (XE image alone is ~11 GB unpacked)."
  type        = number
  default     = 40
}

variable "swap_size_gb" {
  description = "Swap file created on the host as a safety net for 8 GB instances. 0 disables."
  type        = number
  default     = 4
}

variable "bind_address" {
  description = "Host address the container ports are published on. 127.0.0.1 = reachable only via SSH tunnel (recommended); 0.0.0.0 = reachable from anywhere the security group allows."
  type        = string
  default     = "127.0.0.1"

  validation {
    condition     = contains(["127.0.0.1", "0.0.0.0"], var.bind_address)
    error_message = "bind_address must be 127.0.0.1 or 0.0.0.0."
  }
}

############################################
# Software versions
############################################

variable "cp_version" {
  description = "Confluent Platform 8.x image tag for cp-server, cp-schema-registry and cp-kafka-connect."
  type        = string
  default     = "8.3.2"

  validation {
    condition     = can(regex("^8\\.", var.cp_version))
    error_message = "This project targets Confluent Platform 8.x (KRaft only)."
  }
}

variable "oracle_image" {
  description = "Oracle Database 21c Express Edition container image."
  type        = string
  default     = "container-registry.oracle.com/database/express:21.3.0-xe"
}

variable "xstream_connector_version" {
  description = "Version of confluentinc/kafka-connect-oracle-xstream-cdc-source from Confluent Hub."
  type        = string
  default     = "1.6.0"
}

variable "instant_client_url" {
  description = "Oracle Instant Client Basic (Linux x64) zip. Must contain ojdbc8.jar, xstreams.jar and the OCI libraries."
  type        = string
  default     = "https://download.oracle.com/otn_software/linux/instantclient/2119000/instantclient-basic-linux.x64-21.19.0.0.0dbru.zip"
}

variable "docker_compose_version" {
  description = "Docker Compose CLI plugin release installed on the host."
  type        = string
  default     = "v5.5.1"
}

variable "docker_buildx_version" {
  description = "Docker Buildx CLI plugin release (needed by 'docker compose build')."
  type        = string
  default     = "v0.37.1"
}

variable "sample_data_ref" {
  description = "Commit of github.com/ora0600/confluent-new-cdc-connector whose ORDERMGMT sample schema/data scripts are downloaded onto the host."
  type        = string
  default     = "ad5dda5655205ad4175ea6ed539c2100b9abb4a0"
}

############################################
# Oracle / XStream
############################################

variable "oracle_sys_password" {
  description = "Password for SYS / SYSTEM / PDBADMIN of the XE database."
  type        = string
  sensitive   = true

  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9_#!]{7,29}$", var.oracle_sys_password))
    error_message = "8-30 chars, start with a letter, only letters, digits, _ # !"
  }
}

variable "xstream_admin_user" {
  description = "Common user that owns the XStream capture and outbound server."
  type        = string
  default     = "C##GGADMIN"
}

variable "xstream_admin_password" {
  description = "Password of the XStream admin user."
  type        = string
  sensitive   = true

  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9_#!]{7,29}$", var.xstream_admin_password))
    error_message = "8-30 chars, start with a letter, only letters, digits, _ # !"
  }
}

variable "xstream_connect_user" {
  description = "Common user the connector logs in as (outbound server connect_user)."
  type        = string
  default     = "C##CFLTUSER"
}

variable "xstream_connect_password" {
  description = "Password of the XStream connect user."
  type        = string
  sensitive   = true

  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9_#!]{7,29}$", var.xstream_connect_password))
    error_message = "8-30 chars, start with a letter, only letters, digits, _ # !"
  }
}

variable "outbound_server_name" {
  description = "XStream outbound server name."
  type        = string
  default     = "XOUT"
}

variable "capture_name" {
  description = "XStream capture process name."
  type        = string
  default     = "CONFLUENT_XOUT1"
}

variable "checkpoint_retention_days" {
  description = "Capture checkpoint retention (days). Archived logs newer than this are protected from RMAN deletion."
  type        = number
  default     = 1
}

variable "capture_schema" {
  description = "Schema captured by XStream (the sample schema from the reference repo)."
  type        = string
  default     = "ORDERMGMT"
}

variable "capture_tables" {
  description = "Tables in capture_schema that are captured and streamed to Kafka."
  type        = list(string)
  default = [
    "ORDERS", "ORDER_ITEMS", "EMPLOYEES", "PRODUCTS", "CUSTOMERS", "INVENTORIES",
    "PRODUCT_CATEGORIES", "CONTACTS", "NOTES", "WAREHOUSES", "LOCATIONS", "COUNTRIES", "REGIONS",
  ]
}

variable "redo_log_size_mb" {
  description = "Recreate the 3 online redo log groups with this size (XE default is 200 MB). 0 keeps the defaults."
  type        = number
  default     = 512
}

variable "recovery_area_size_gb" {
  description = "db_recovery_file_dest_size. Archived redo lands here; the DB stalls when it is full."
  type        = number
  default     = 20
}

############################################
# Confluent Platform / connector
############################################

variable "kafka_cluster_id" {
  description = "KRaft cluster id (base64 UUID, 22 chars). Generate with: kafka-storage random-uuid"
  type        = string
  default     = "MkU3OEVBNTcwNTJENDM2Qk"
}

variable "kafka_heap" {
  description = "KAFKA_HEAP_OPTS for the broker."
  type        = string
  default     = "-Xms512m -Xmx1g"
}

variable "schema_registry_heap" {
  description = "SCHEMA_REGISTRY_HEAP_OPTS."
  type        = string
  default     = "-Xms256m -Xmx384m"
}

variable "connect_heap" {
  description = "KAFKA_HEAP_OPTS for the Connect worker."
  type        = string
  default     = "-Xms512m -Xmx1536m"
}

variable "connector_name" {
  description = "Name of the XStream source connector instance."
  type        = string
  default     = "XSTREAMCDC0"
}

variable "topic_prefix" {
  description = "Topic prefix; topics are <prefix>.<SCHEMA>.<TABLE>."
  type        = string
  default     = "XEPDB1"
}

variable "jdbc_connector_version" {
  description = "Version of confluentinc/kafka-connect-jdbc from Confluent Hub (bundles ojdbc8). Always baked into the Connect image."
  type        = string
  default     = "10.9.9"
}

############################################
# Control Center (next-gen, CP 8.x)
############################################

variable "enable_control_center" {
  description = "Run Confluent Control Center with its Prometheus + Alertmanager (~2 GB RAM). UI on port 9021."
  type        = bool
  default     = true
}

variable "control_center_version" {
  description = "Image tag for cp-enterprise-control-center-next-gen, cp-enterprise-prometheus and cp-enterprise-alertmanager."
  type        = string
  default     = "2.5.0"
}

variable "control_center_heap" {
  description = "CONTROL_CENTER_HEAP_OPTS."
  type        = string
  default     = "-Xms512m -Xmx1536m"
}

############################################
# JDBC sink (Kafka -> Oracle)
############################################

variable "enable_jdbc_sink" {
  description = "Deploy a JDBC sink connector that replays the CDC topics into sink_schema in the same XE PDB."
  type        = bool
  default     = true
}

variable "sink_schema" {
  description = "Oracle user/schema (in XEPDB1) the JDBC sink writes to. Created if missing; tables are auto-created."
  type        = string
  default     = "ORDERMGMT_SINK"
}

variable "sink_schema_password" {
  description = "Password of sink_schema."
  type        = string
  sensitive   = true
  default     = ""

  validation {
    condition     = var.sink_schema_password == "" || can(regex("^[A-Za-z][A-Za-z0-9_#!]{7,29}$", var.sink_schema_password))
    error_message = "8-30 chars, start with a letter, only letters, digits, _ # !"
  }
}

variable "jdbc_sink_connector_name" {
  description = "Name of the JDBC sink connector instance."
  type        = string
  default     = "JDBCSINK0"
}

variable "topic_retention_ms" {
  description = "retention.ms for the CDC topics and the broker default for new topics. -1 = keep forever."
  type        = number
  default     = -1

  validation {
    condition     = var.topic_retention_ms == -1 || var.topic_retention_ms > 0
    error_message = "Use -1 (infinite) or a positive number of milliseconds."
  }
}

variable "confluent_license" {
  description = "Confluent license key. Empty = 30-day trial (fine for a lab)."
  type        = string
  default     = ""
  sensitive   = true
}

############################################
# Behaviour
############################################

variable "run_validation" {
  description = "Run bin/validate.sh (incl. an end-to-end insert -> Kafka check) at the end of apply."
  type        = bool
  default     = true
}

variable "destroy_purges_data" {
  description = "On 'terraform destroy', also delete Docker volumes (database + Kafka data) and the remote directory."
  type        = bool
  default     = false
}
