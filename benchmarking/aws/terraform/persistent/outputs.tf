output "dashboard_urls" {
  value = {
    for k, d in aws_cloudwatch_dashboard.soak :
    k => "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards/dashboard/${d.dashboard_name}"
  }
}
