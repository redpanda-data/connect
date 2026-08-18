# Soak alerting (CON-179 R6, increment 4): the three failure shapes the last
# six months of incidents taught us to watch for, alarmed per soak scenario.
# Alarms key off the emitter contract in runner/cloudwatch.go. All alarms
# treat missing data as notBreaching: no run in progress means silence, not
# a page.

resource "aws_sns_topic" "soak_alerts" {
  name = "redpanda-connect-bench-soak-alerts"
}

variable "soak_alert_email" {
  # Endpoint for soak alarm notifications. SNS sends a confirmation email on
  # first apply — the subscription is inactive until the link is clicked.
  # Swap for AWS Chatbot / Slack later without touching the alarms.
  type    = string
  default = "prakhar.garg@redpanda.com"
}

resource "aws_sns_topic_subscription" "soak_alerts_email" {
  topic_arn = aws_sns_topic.soak_alerts.arn
  protocol  = "email"
  endpoint  = var.soak_alert_email
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
  alarm_description   = "Soak throughput <= 0.5 MB/s for 10 minutes while the run is active — the silent-stall class (#4648/#4655)."
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
# (RSSSlopeBytesPerMin); 2 MB/min sustained for 15 minutes is far above the
# healthy baseline (~0.07 MB/min measured on the first clean soak) while
# still catching an OOM-in-hours leak early in a 90-minute window.
resource "aws_cloudwatch_metric_alarm" "soak_rss_slope" {
  for_each            = local.soak_alarm_dims
  alarm_name          = "rpcn-soak-${each.key}-rss-slope"
  alarm_description   = "Soak RSS climbing >= 2 MB/min sustained — the slow-leak class (inc-2861/#4527/#4657)."
  namespace           = "RedpandaConnect/Bench"
  metric_name         = "RSSSlopeBytesPerMin"
  dimensions          = each.value
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
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
  alarm_description   = "Soak end-to-end backlog >= 600s for 10 minutes — the connector is falling behind its source."
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
