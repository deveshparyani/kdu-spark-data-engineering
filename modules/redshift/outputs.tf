output "namespace_name" {
  description = "Redshift namespace name."
  value       = aws_redshiftserverless_namespace.this.namespace_name
}

output "workgroup_name" {
  description = "Redshift workgroup name."
  value       = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "admin_secret_arn" {
  description = "Secrets Manager ARN containing Redshift admin credentials."
  value       = aws_secretsmanager_secret.admin.arn
  sensitive   = true
}

output "database_name" {
  description = "Redshift database name."
  value       = var.database_name
}
