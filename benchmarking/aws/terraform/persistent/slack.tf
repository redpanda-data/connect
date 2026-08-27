# Slack delivery for soak alarms and orphan-reaper notices via AWS Chatbot
# (console name: "Amazon Q Developer in chat applications").
#
# One-time manual prerequisite (cannot be Terraform'd): authorize the Slack
# workspace in the AWS Chatbot console of this account (OAuth; may need a
# Slack workspace admin to approve the AWS app). That yields the workspace
# ID committed as the default below, alongside the channel's ID (from the
# channel's "About" tab). Passing both as "" turns Slack delivery off;
# alarms.tf's validation then requires the email subscriber instead, so
# alerts can never be silently unrouted.
#
# CloudWatch alarm messages render natively as alarm cards. The reaper's
# notices are published in Chatbot's custom-notification schema via SNS
# per-protocol messages (see cleanup-lambda/sweep.go) — Chatbot silently
# drops plain-text SNS payloads, so without that the reaper channel would
# look connected while delivering nothing.

# The real IDs are committed as defaults (they are not secrets — both appear
# in every Slack URL), mirroring backend.hcl's account-specific values:
# change them in a private fork if you run this elsewhere. Committed
# defaults, not env vars, because a count-gated resource driven by ambient
# TF_VAR_* would be silently DESTROYED by any re-apply whose shell didn't
# re-export them — the primary alert channel vanishing with no error.
# Explicitly passing both as "" disables Slack delivery (the email
# validation in alarms.tf then requires the backup subscriber instead).

variable "slack_workspace_id" {
  description = "Slack workspace (team) ID from the AWS Chatbot console OAuth. Set with slack_channel_id; both empty disables Slack delivery."
  type        = string
  default     = "TPMVB7YMC" # Redpanda workspace (authorized 2026-08-27)
}

variable "slack_channel_id" {
  description = "Slack channel ID for soak alarms + reaper notices. Set with slack_workspace_id; both empty disables Slack delivery."
  type        = string
  default     = "C0BT00TTA11" # #soak-redpanda-connect

  validation {
    condition     = (var.slack_channel_id == "") == (var.slack_workspace_id == "")
    error_message = "slack_workspace_id and slack_channel_id must be set together (or both passed as empty to disable Slack delivery)."
  }
}

locals {
  slack_enabled = var.slack_workspace_id != "" && var.slack_channel_id != ""
}

# Channel guardrail role: what Chatbot may do ON BEHALF OF channel members.
# Read-only CloudWatch is all the alarm cards need (rendering the metric
# graph); nothing in this channel should ever mutate the account.
resource "aws_iam_role" "chatbot_channel" {
  count = local.slack_enabled ? 1 : 0
  name  = "rpcn-bench-chatbot-channel"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "chatbot.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "chatbot_channel_cw_read" {
  count      = local.slack_enabled ? 1 : 0
  role       = aws_iam_role.chatbot_channel[0].name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess"
}

resource "aws_chatbot_slack_channel_configuration" "soak_alerts" {
  count              = local.slack_enabled ? 1 : 0
  configuration_name = "rpcn-bench-soak-alerts"
  iam_role_arn       = aws_iam_role.chatbot_channel[0].arn
  slack_team_id      = var.slack_workspace_id
  slack_channel_id   = var.slack_channel_id
  # Guardrail caps every action in the channel regardless of the role above.
  guardrail_policy_arns = ["arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess"]
  sns_topic_arns = [
    aws_sns_topic.soak_alerts.arn,
    aws_sns_topic.orphan_cleanup.arn,
  ]
  logging_level = "ERROR"
}
