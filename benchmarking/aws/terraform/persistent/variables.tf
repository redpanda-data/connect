variable "region" {
  type    = string
  default = "us-east-2"
}

variable "orphan_ttl_hours" {
  description = "How long a tagged bench resource can live before the cleanup Lambda destroys it. 4 because the postgres 4-vCPU sweep with both engines takes ~2.5-3 hours (2026-05-29). NOTE: a >4h soak needs the bench-soak-until tag exemption (increment 3) before it can run."
  type        = number
  default     = 4
}

variable "soak_scenarios" {
  # Adding a connector to the soak rotation = one more entry here (plus its
  # soak scenario YAML). The key becomes the dashboard-name suffix.
  type = map(object({
    connector = string
    scenario  = string
  }))
  default = {
    postgres = {
      connector = "postgres_cdc"
      scenario  = "postgres-orders-soak"
    }
    mysql = {
      connector = "mysql_cdc"
      scenario  = "mysql-orders-soak"
    }
  }
}

variable "bench_catalog_gcp_sa_unique_id" {
  # Numeric unique ID of the catalogue app's Cloud Run runtime service account
  # (bench-catalog-sa@rp-internal-agents.iam.gserviceaccount.com). The role
  # below exists only when this is set.
  type    = string
  default = ""
}

variable "bench_catalog_gcp_audience" {
  # Audience the app requests on its Google ID token; the role trusts only it.
  type    = string
  default = "redpanda-connect-bench-catalog"
}
