output "broker_endpoints" {
  description = "Comma-separated host:9092 list, suitable as Kafka bootstrap.servers."
  value       = join(",", [for ip in var.broker_ips : "${ip}:9092"])
}

output "metrics_endpoint" {
  description = "First broker's host:9644 — scraping point for Redpanda Prometheus metrics."
  value       = "${var.broker_ips[0]}:9644"
}

output "metrics_endpoints" {
  description = "Comma-separated host:9644 list — scrape all brokers because Redpanda's per-topic byte metrics are per-broker."
  value       = join(",", [for ip in var.broker_ips : "${ip}:9644"])
}

output "broker_sg_id" {
  description = "Broker security group ID — for downstream ingress rules from new client SGs."
  value       = aws_security_group.broker.id
}

output "schema_registry_url" {
  description = "Base URL of the first broker's built-in Schema Registry. Every broker serves the same _schemas topic, so any one works; clients that need failover can use schema_registry_urls."
  value       = "http://${var.broker_ips[0]}:8081"
}

output "schema_registry_urls" {
  description = "Comma-separated base URLs of every broker's Schema Registry."
  value       = join(",", [for ip in var.broker_ips : "http://${ip}:8081"])
}
