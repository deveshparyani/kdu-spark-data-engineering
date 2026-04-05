output "data_lake_bucket_name" {
  description = "Name of the S3 data lake bucket."
  value       = module.s3.bucket_name
}

output "upload_processor_lambda_name" {
  description = "Upload processor Lambda name."
  value       = module.lambda_upload_processor.function_name
}

output "adscribe_ingestion_lambda_name" {
  description = "Adscribe ingestion Lambda name."
  value       = module.lambda_adscribe_ingestion.function_name
}

output "step_function_arn" {
  description = "State machine ARN."
  value       = module.step_function.state_machine_arn
}

output "dynamodb_table_name" {
  description = "Idempotency DynamoDB table name."
  value       = module.dynamodb.table_name
}

output "lock_table_name" {
  description = "Distributed lock DynamoDB table name."
  value       = module.lock_table.table_name
}

output "ingestion_queue_name" {
  description = "Primary ingestion SQS queue name."
  value       = module.sqs_ingestion.queue_name
}

output "config_validator_lambda_name" {
  description = "Config validator Lambda name."
  value       = module.lambda_config_validator.function_name
}

output "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name."
  value       = module.redshift.workgroup_name
}

output "redshift_admin_secret_arn" {
  description = "Secrets Manager ARN containing Redshift admin credentials."
  value       = module.redshift.admin_secret_arn
  sensitive   = true
}

output "adscribe_secret_arn" {
  description = "Secrets Manager ARN used by the Adscribe ingestion Lambda."
  value       = aws_secretsmanager_secret.adscribe.arn
}
