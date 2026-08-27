# Soak alerting (CON-179 R6, increment 4): the three failure shapes the last
# six months of incidents taught us to watch for, alarmed per soak scenario.
# Alarms key off the emitter contract in runner/cloudwatch.go. All alarms
# treat missing data as notBreaching: no run in progress means silence, not
# a page.

resource "aws_sns_topic" "soak_alerts" {
  name = "redpanda-connect-bench-soak-alerts"
}

# Email backup subscriptions are deliberately NOT managed here. Terraform
# manages exactly one alert channel — Slack (slack.tf, committed IDs) — and
# email backups are manual, out-of-band subscriptions (see SOAK.md):
#
#   aws sns subscribe --topic-arn <soak-alerts|orphans topic> \
#     --protocol email --notification-endpoint <team-alias>
#
# Why not a Terraform resource: an address can't be committed (no team
# alias exists; a personal one doesn't belong in the repo), and a
# subscription count-gated on an ambient TF_VAR_* is silently destroyed by
# any re-apply whose shell didn't re-export it — worse than unmanaged,
# because resurrecting an SNS email subscription needs a human to re-click
# the confirmation link. Manual subscriptions survive every apply.
#
# The pre-existing email subscription in 605419575229 is preserved as
# exactly such an unmanaged subscription by the removed block below (state
# forget, not destroy).
removed {
  from = aws_sns_topic_subscription.soak_alerts_email

  lifecycle {
    destroy = false
  }
}

locals {
  soak_alarm_dims = {
    for k, v in var.soak_scenarios : k => {
      Connector = v.connector
      Scenario  = v.scenario
    }
  }
}

# Shape 1 — the stall (#4648, #4655): throughput at zero while the run is
# live. The '"reads as working" while delivering nothing' class.
resource "aws_cloudwatch_metric_alarm" "soak_stall" {
  for_each            = local.soak_alarm_dims
  alarm_name          = "rpcn-soak-${each.key}-stall"
  alarm_description   = "STALL: the soak connector stopped moving data (throughput ~0 for 10+ min during an active run). It usually still LOOKS healthy — process up, health checks green — so don't trust 'connected'. First: open the rpcn-bench-soak dashboard, confirm a run is actually in progress, then pull the run's log. Runbook: benchmarking/aws/SOAK.md. Past examples: github.com/redpanda-data/connect/issues/4648 (input silently stalls), /4655 (health endpoint lies)."
  namespace           = "RedpandaConnect/Bench"
  metric_name         = "ThroughputMBps"
  dimensions          = each.value
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 2
  threshold           = 0.5
  comparison_operator = "LessThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.soak_alerts.arn]
  ok_actions          = [aws_sns_topic.soak_alerts.arn]
}

# Shape 2 — the slow leak (inc-2861, #4527, #4657): sustained positive RSS
# slope. The metric is least-squares-fitted by the orchestrator
# (RSSSlopeBytesPerMin); 2 MB/min sustained is far above the healthy
# baseline (~0.07 MB/min measured on the first clean soak) while still
# catching an OOM-in-hours leak early in a 90-minute window.
#
# Unlike ThroughputMBps/BacklogSeconds (backfilled per minute), this metric
# is emitted once per checkpoint cycle — every 600s for a soak
# (soakCheckpointSec, runner/main.go) — so the period must match that
# cadence: a finer period leaves empty periods between datapoints that
# notBreaching evaluates as OK, resetting any consecutive-breach streak and
# making the alarm unreachable. 2-of-3 datapoints_to_alarm pages after two
# breaching checkpoints (~20 min of sustained leak) while tolerating one
# datapoint straddling a period boundary.
resource "aws_cloudwatch_metric_alarm" "soak_rss_slope" {
  for_each            = local.soak_alarm_dims
  alarm_name          = "rpcn-soak-${each.key}-rss-slope"
  alarm_description   = "MEMORY LEAK: the soak connector's memory (RSS) is climbing >= 2 MB/min sustained across checkpoints. Sounds small but that's ~3 GB/day — in production this OOM-kills within days. First: open the Memory widget on the rpcn-bench-soak dashboard and look at the RSS slope shape. Runbook: benchmarking/aws/SOAK.md. Past examples: github.com/redpanda-data/connect/issues/4527 (protobuf profile leak), /4657 (growth under back-pressure)."
  namespace           = "RedpandaConnect/Bench"
  metric_name         = "RSSSlopeBytesPerMin"
  dimensions          = each.value
  statistic           = "Average"
  period              = 600
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  threshold           = 2000000
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.soak_alerts.arn]
  ok_actions          = [aws_sns_topic.soak_alerts.arn]
}

# Shape 3 — falling behind (#4489-adjacent): backlog vs the source growing
# past 10 minutes and staying there. A healthy soak holds a small constant
# backlog (~1-2 min observed); sustained growth means the connector cannot
# keep up or has quietly stopped acking.
resource "aws_cloudwatch_metric_alarm" "soak_backlog" {
  for_each            = local.soak_alarm_dims
  alarm_name          = "rpcn-soak-${each.key}-backlog"
  alarm_description   = "BACKLOG: the soak connector has fallen 10+ minutes behind the database it replicates and stayed there. Data IS flowing and memory is fine — it's just slower than the source is writing, so the gap grows without bound (in production: a replica going hours stale). First: check BacklogSeconds vs Records/s on the rpcn-bench-soak dashboard — flat-but-high backlog means a one-time hiccup, climbing means it's losing the race. Runbook: benchmarking/aws/SOAK.md."
  namespace           = "RedpandaConnect/Bench"
  metric_name         = "BacklogSeconds"
  dimensions          = each.value
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 600
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.soak_alerts.arn]
  ok_actions          = [aws_sns_topic.soak_alerts.arn]
}

output "soak_alerts_topic_arn" { value = aws_sns_topic.soak_alerts.arn }
