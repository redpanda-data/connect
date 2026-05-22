output "broker_endpoints" {
  description = "Comma-separated host:9092 list, suitable as Kafka bootstrap.servers."
  value       = join(",", [for ip in var.broker_ips : "${ip}:9092"])
}

output "metrics_endpoint" {
  description = "First broker's host:9644 — scraping point for Redpanda Prometheus metrics."
  value       = "${var.broker_ips[0]}:9644"
}

output "broker_sg_id" {
  description = "Broker security group ID — for downstream ingress rules from new client SGs."
  value       = aws_security_group.broker.id
}
