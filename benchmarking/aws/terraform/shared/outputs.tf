output "runner_instance_id"   { value = aws_instance.runner.id }
output "load_gen_instance_id" { value = aws_instance.load_gen.id }
output "vpc_id"               { value = aws_vpc.main.id }
output "private_subnet_ids"   { value = aws_subnet.private[*].id }
output "public_subnet_ids"    { value = aws_subnet.public[*].id }
output "results_bucket"       { value = aws_s3_bucket.results.bucket }
