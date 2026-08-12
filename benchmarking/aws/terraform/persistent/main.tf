# Persistent bench resources — CloudWatch dashboards (and, later, alarms +
# the alerting SNS topic) that must OUTLIVE bench sessions.
#
# Everything in shared/ and stacks/ is created per run and destroyed at
# teardown; this root is deliberately outside that lifecycle. Apply it once
# (and re-apply on change) via `task aws:persistent` — it is never touched
# by `task aws:down` or the orphan-cleanup Lambda (which sweeps only
# EC2/RDS/S3/IAM).
#
# State key: persistent (task aws:persistent passes
# -backend-config=key=persistent alongside backend.hcl).

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
  backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project   = "redpanda-connect-bench"
      Stack     = "persistent"
      ManagedBy = "terraform"
    }
  }
}

locals {
  namespace = "RedpandaConnect/Bench"
}

# One dashboard per soak scenario (see variables.tf to add a connector to
# the rotation). Widget metrics match the emitter contract in
# runner/cloudwatch.go exactly: namespace RedpandaConnect/Bench, dimensions
# Connector + Scenario.
resource "aws_cloudwatch_dashboard" "soak" {
  for_each       = var.soak_scenarios
  dashboard_name = "rpcn-bench-soak-${each.key}"

  dashboard_body = jsonencode({
    widgets = [
      {
        type = "text", x = 0, y = 0, width = 24, height = 2
        properties = {
          markdown = "# Soak: ${each.value.scenario} (${each.value.connector})\nSustained-load leak/stall/rotation watch (CON-179 R6). Data arrives in ~10-minute batches — the orchestrator publishes from S3 checkpoints, so a live run trails real time by up to one checkpoint interval."
        }
      },
      {
        type = "metric", x = 0, y = 2, width = 12, height = 6
        properties = {
          title  = "Throughput (broker vs Connect log)"
          region = var.region, period = 60, stat = "Average"
          metrics = [
            [local.namespace, "ThroughputMBps", "Connector", each.value.connector, "Scenario", each.value.scenario],
            [local.namespace, "LogThroughputMBps", "Connector", each.value.connector, "Scenario", each.value.scenario],
          ]
          yAxis = { left = { min = 0, label = "MB/s" } }
        }
      },
      {
        type = "metric", x = 12, y = 2, width = 12, height = 6
        properties = {
          title  = "Memory (RSS is what the OOM killer sees)"
          region = var.region, period = 60, stat = "Maximum"
          metrics = [
            [local.namespace, "RSSBytes", "Connector", each.value.connector, "Scenario", each.value.scenario],
            [local.namespace, "HeapInUseBytes", "Connector", each.value.connector, "Scenario", each.value.scenario],
          ]
          yAxis = { left = { min = 0, label = "bytes" } }
        }
      },
      {
        type = "metric", x = 0, y = 8, width = 8, height = 6
        properties = {
          title  = "Goroutines"
          region = var.region, period = 60, stat = "Maximum"
          metrics = [
            [local.namespace, "Goroutines", "Connector", each.value.connector, "Scenario", each.value.scenario],
          ]
          yAxis = { left = { min = 0 } }
        }
      },
      {
        type = "metric", x = 8, y = 8, width = 8, height = 6
        properties = {
          title  = "End-to-end backlog (seconds behind source)"
          region = var.region, period = 60, stat = "Maximum"
          metrics = [
            [local.namespace, "BacklogSeconds", "Connector", each.value.connector, "Scenario", each.value.scenario],
          ]
          yAxis = { left = { min = 0, label = "s" } }
        }
      },
      {
        type = "metric", x = 16, y = 8, width = 8, height = 6
        properties = {
          title  = "Records/s + run liveness"
          region = var.region, period = 60, stat = "Average"
          metrics = [
            [local.namespace, "RecordsPerSec", "Connector", each.value.connector, "Scenario", each.value.scenario],
            [local.namespace, "RunActive", "Connector", each.value.connector, "Scenario", each.value.scenario, { yAxis = "right" }],
          ]
          yAxis = { left = { min = 0 }, right = { min = 0, max = 2, showUnits = false } }
        }
      },
    ]
  })
}
