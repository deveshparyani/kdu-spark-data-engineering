variable "project_name" {
  description = "Project prefix."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "region" {
  description = "AWS region."
  type        = string
}

variable "account_id" {
  description = "AWS account ID."
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket name."
  type        = string
}

variable "bucket_arn" {
  description = "S3 bucket ARN."
  type        = string
}

variable "raw_prefix" {
  description = "S3 raw prefix."
  type        = string
}

variable "processed_prefix" {
  description = "S3 processed prefix."
  type        = string
}

variable "curated_prefix" {
  description = "S3 curated prefix."
  type        = string
}

variable "quarantine_prefix" {
  description = "S3 quarantine prefix."
  type        = string
}

variable "configs_prefix" {
  description = "S3 configs prefix."
  type        = string
}

variable "scripts_prefix" {
  description = "S3 scripts prefix."
  type        = string
}

variable "temp_prefix" {
  description = "S3 temp prefix."
  type        = string
}

variable "dynamodb_table_arn" {
  description = "DynamoDB table ARN."
  type        = string
}

variable "readiness_table_arn" {
  description = "DynamoDB readiness table ARN."
  type        = string
}

variable "lock_table_arn" {
  description = "DynamoDB lock table ARN."
  type        = string
}

variable "ingestion_queue_arn" {
  description = "Primary ingestion SQS queue ARN."
  type        = string
}

variable "upload_lambda_name" {
  description = "Upload processor Lambda name."
  type        = string
}

variable "adscribe_lambda_name" {
  description = "Adscribe ingestion Lambda name."
  type        = string
}

variable "config_validator_lambda_name" {
  description = "Config validator Lambda name."
  type        = string
}

variable "state_machine_name" {
  description = "Step Functions state machine name."
  type        = string
}

variable "glue_job_names" {
  description = "Glue job names used by the state machine."
  type        = list(string)
}

variable "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name."
  type        = string
}

variable "adscribe_secret_name" {
  description = "Secrets Manager secret name for Adscribe credentials."
  type        = string
}

variable "redshift_secret_name" {
  description = "Secrets Manager secret name for Redshift credentials."
  type        = string
}

variable "lambda_upload_role_name" {
  description = "IAM role name for the upload processor Lambda."
  type        = string
}

variable "lambda_adscribe_role_name" {
  description = "IAM role name for the Adscribe Lambda."
  type        = string
}

variable "lambda_validator_role_name" {
  description = "IAM role name for the config validator Lambda."
  type        = string
}

variable "glue_role_name" {
  description = "IAM role name for Glue jobs."
  type        = string
}

variable "step_function_role_name" {
  description = "IAM role name for Step Functions."
  type        = string
}

variable "redshift_copy_role_name" {
  description = "IAM role name for Redshift COPY access."
  type        = string
}

variable "tags" {
  description = "Base tags applied to supported resources."
  type        = map(string)
  default     = {}
}
