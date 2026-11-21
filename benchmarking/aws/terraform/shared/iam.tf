data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bench_host" {
  name               = "${local.name_prefix}-host"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.bench_host.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Scoped to the session results bucket ONLY. This role is what an approved
# /soak PR binary runs under, so its reach defines the blast radius of
# untrusted-but-authorized code: it must not be able to touch the persistent
# soak archive (forged baselines), tfstate, or secrets (the sink-secret grant
# below is attached only for e2e runs, which never run PR binaries). The license reaches the host via the staged S3 object,
# not Secrets Manager (the orchestrator fetches the secret). dynamodb:*
# returns, scoped, with the stack that needs it (dynamodb benches — not in
# this tree). The Glue grant below is the iceberg sink's: Connect on this host
# reaches the Glue Iceberg REST catalog as this role, fenced to the bench_*
# scratch databases the orchestrator creates per run.
resource "aws_iam_role_policy" "bench_host_extra" {
  role = aws_iam_role.bench_host.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
        ]
        Resource = ["${aws_s3_bucket.results.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.results.arn]
      },
    ]
  })
}

data "aws_caller_identity" "current" {}

# The iceberg sink's catalog access. The Glue Iceberg REST endpoint authorises
# each call against the catalog, the database and the table, so all three
# ARN shapes are listed. Only bench_* databases are reachable: the persistent
# stack and any customer database are out of reach of an approved PR binary.
resource "aws_iam_role_policy" "bench_host_glue" {
  role = aws_iam_role.bench_host.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetCatalog",
          "glue:CreateDatabase", "glue:GetDatabase", "glue:GetDatabases", "glue:DeleteDatabase",
          "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/bench_*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/bench_*/*",
        ]
      },
    ]
  })
}

# The e2e sinks' credential secrets (snowflake and bigquery).
# The bench script fetches the key on the host and writes it to a 0600 file
# for Connect, so the key never passes through the orchestrator or an SSM
# parameter. The grant exists only when sink_secrets_enabled is true, which
# the runner sets only for an e2e run whose sink needs a secret; e2e runs
# never take --binary, so a PR binary never runs under a role that can read
# it. The -?????? matches the random suffix Secrets Manager appends to the
# name, so exactly one secret per sink is readable.
resource "aws_iam_role_policy" "bench_host_sink_secrets" {
  count = var.sink_secrets_enabled ? 1 : 0
  role  = aws_iam_role.bench_host.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = [
          "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:redpanda-connect-bench/snowflake-??????",
          "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:redpanda-connect-bench/bigquery-??????",
        ]
      },
    ]
  })
}

resource "aws_iam_instance_profile" "bench_host" {
  name = "${local.name_prefix}-host"
  role = aws_iam_role.bench_host.name
}
