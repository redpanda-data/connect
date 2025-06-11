output "database_connection_string" {
  description = "Connection string for the Spanner database"
  value       = "projects/${var.project_id}/instances/${google_spanner_instance.main.name}/databases/${google_spanner_database.database.name}"
}

output "instance_state" {
  description = "The current state of the Spanner instance"
  value       = google_spanner_instance.main.state
}
