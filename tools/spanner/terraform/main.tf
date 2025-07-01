terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
  required_version = ">= 1.0"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "current" {}

resource "google_spanner_instance" "main" {
  name         = var.instance_name
  config       = var.instance_config
  display_name = var.instance_display_name
  num_nodes    = var.instance_nodes

  labels = {
    environment = var.environment
  }
}

resource "google_spanner_database" "database" {
  instance = google_spanner_instance.main.name
  name     = var.database_name

  deletion_protection = false
  version_retention_period = "1h"  # Disable backups by setting retention to minimum
  enable_drop_protection = false
}
