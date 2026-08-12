variable "region" {
  type    = string
  default = "us-east-2"
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
  }
}
