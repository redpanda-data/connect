# GitHub Actions OIDC federation for scheduled soak runs (CON-179 R6,
# increment 3). No long-lived keys anywhere: the workflow assumes the
# provisioner role via OIDC for the duration of one run.
#
# Session duration is 4h (role max_session_duration + the workflow's
# role-duration-seconds) because the ~2h20m nightly soak must never outlive
# its credentials — expiring mid-run blinds the orchestrator and strands
# infrastructure, as two laptop runs proved on 2026-08-12.

# One OIDC provider per account. If the account already has one for GitHub
# (e.g. created by another team), import it instead of applying this:
#   terraform import aws_iam_openid_connect_provider.github <arn>
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # AWS validates GitHub's OIDC tokens against its own trust store and
  # ignores this thumbprint for token.actions.githubusercontent.com, but the
  # field is required; this is GitHub's well-known root CA thumbprint.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

variable "github_trusted" {
  # repo → branch refs whose workflows may assume the provisioner role.
  # Keep this SHORT — every entry is a branch whose workflow code runs with
  # the permissions below, and personal forks must never appear here: they
  # carry none of the org's branch protection, SSO, or audit controls.
  type = map(list(string))
  default = {
    "redpanda-data/connect" = ["refs/heads/main"]
  }
}

resource "aws_iam_role" "gha_provisioner" {
  name                 = "rpcn-bench-gha-provisioner"
  max_session_duration = 14400 # 4h — see header comment
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          "token.actions.githubusercontent.com:sub" = [
            for sub in flatten([
              for repo, refs in var.github_trusted : [
                for ref in refs : "repo:${repo}:ref:${ref}"
              ]
            ]) : sub
          ]
        }
      }
    }]
  })
}

# The provisioner runs terraform for the whole session stack: VPC, EC2, RDS,
# IAM roles, S3, SSM, CloudWatch. That legitimately needs administrative
# breadth, and this account is dedicated to disposable bench infrastructure
# with no customer data — the real guardrails are the pinned OIDC trust
# above, the budget alarm, and the orphan reaper. A permissions boundary to
# formally fence this role is a tracked follow-up for the org-repo
# migration.
resource "aws_iam_role_policy_attachment" "gha_provisioner_admin" {
  role       = aws_iam_role.gha_provisioner.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

# Soak results archive — outlives every session stack. The per-session
# results bucket is force_destroy'd at teardown, which nearly cost us the
# first soak's data; result.json, the soak-index entries (increment 4's
# rolling-baseline food), and copies of the raw per-point artifacts land
# here instead.
resource "aws_s3_bucket" "soak_archive" {
  bucket = "redpanda-connect-bench-soak-archive"
}

resource "aws_s3_bucket_public_access_block" "soak_archive" {
  bucket                  = aws_s3_bucket.soak_archive.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "soak_archive" {
  bucket = aws_s3_bucket.soak_archive.id
  rule {
    id     = "expire-raw-artifacts"
    status = "Enabled"
    # runs/ holds the bulky per-run payloads (raw logs, prom/broker dumps,
    # result.json) — 180 days matches the old session bucket's policy. The
    # tiny soak-index/ entries (the rolling-baseline input) are outside this
    # prefix and kept indefinitely.
    filter { prefix = "runs/" }
    expiration { days = 180 }
  }
}

output "gha_provisioner_role_arn" { value = aws_iam_role.gha_provisioner.arn }
output "soak_archive_bucket" { value = aws_s3_bucket.soak_archive.bucket }
