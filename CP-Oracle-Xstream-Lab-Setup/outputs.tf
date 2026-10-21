output "ssh" {
  description = "SSH into the host."
  value       = "ssh -i ${var.ssh_private_key_path} ${var.ssh_user}@${var.host}"
}

output "ssh_tunnel" {
  description = "Forward Oracle, Kafka, Schema Registry, Connect REST (and Control Center) to your laptop (needed when bind_address = 127.0.0.1)."
  value       = "ssh -i ${var.ssh_private_key_path} -N -L 1521:localhost:1521 -L 9092:localhost:9092 -L 8081:localhost:8081 -L 8083:localhost:8083${var.enable_control_center ? " -L 9021:localhost:9021" : ""} ${var.ssh_user}@${var.host}"
}

output "validate" {
  description = "Re-run the validation script on the host."
  value       = "ssh -i ${var.ssh_private_key_path} ${var.ssh_user}@${var.host} 'sudo bash ${var.remote_dir}/bin/validate.sh'"
}

output "connector_status" {
  description = "Connector status (run on the host, or locally through the tunnel)."
  value       = "curl -s http://localhost:8083/connectors/${var.connector_name}/status | jq"
}

output "kafka_topics" {
  description = "CDC topics produced by the connector."
  value       = [for t in var.capture_tables : "${var.topic_prefix}.${upper(var.capture_schema)}.${upper(t)}"]
}

output "oracle_connections" {
  description = "sqlplus connect strings (from the host: sudo docker exec -it oracle21c sqlplus <string>)."
  value = {
    sys_cdb     = "sys/<oracle_sys_password>@//localhost:1521/XE as sysdba"
    sys_pdb     = "sys/<oracle_sys_password>@//localhost:1521/XEPDB1 as sysdba"
    sample_user = "ordermgmt/kafka@//localhost:1521/XEPDB1"
    xstream_adm = "${upper(var.xstream_admin_user)}/<xstream_admin_password>@//localhost:1521/XE"
  }
}

output "control_center_url" {
  description = "Control Center UI (through the ssh_tunnel when bind_address = 127.0.0.1)."
  value       = var.enable_control_center ? "http://${var.bind_address == "127.0.0.1" ? "localhost" : var.host}:9021" : "disabled"
}

output "jdbc_sink" {
  description = "JDBC sink connector and the Oracle schema it writes to."
  value = var.enable_jdbc_sink ? {
    connector = var.jdbc_sink_connector_name
    status    = "curl -s http://localhost:8083/connectors/${var.jdbc_sink_connector_name}/status | jq"
    target    = "${upper(var.sink_schema)}@//localhost:1521/XEPDB1"
  } : null
}
