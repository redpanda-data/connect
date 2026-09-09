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
# soak archive (forged baselines), tfstate, or secrets. The license reaches
# the host via the staged S3 object, not Secrets Manager (the orchestrator
# fetches the secret). glue:*/dynamodb:* return, scoped, with the stacks
# that need them (iceberg, dynamodb benches — not in this tree).
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

resource "aws_iam_instance_profile" "bench_host" {
  name = "${local.name_prefix}-host"
  role = aws_iam_role.bench_host.name
}
