variable "name" {
  description = "State machine name."
  type        = string
}

variable "role_arn" {
  description = "IAM role ARN used by the state machine."
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 30
}

variable "config_validator_lambda_arn" {
  description = "ARN of the config validator Lambda."
  type        = string
}

variable "glue_silver_job_name" {
  description = "Silver Glue job name."
  type        = string
}

variable "glue_gold_job_name" {
  description = "Gold Glue job name."
  type        = string
}

variable "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name."
  type        = string
}

variable "redshift_database" {
  description = "Redshift database name."
  type        = string
}

variable "redshift_secret_arn" {
  description = "Secrets Manager ARN used for Redshift Data API authentication."
  type        = string
}

variable "lock_table_name" {
  description = "DynamoDB table name used for distributed locking."
  type        = string
}

variable "redshift_target_table" {
  description = "Target Redshift fact table."
  type        = string
}

variable "redshift_staging_table" {
  description = "Staging table used during Redshift loads."
  type        = string
}

variable "copy_role_arn" {
  description = "IAM role ARN used by the COPY command."
  type        = string
}

variable "redshift_loader_lambda_arn" {
  description = "ARN of the Redshift loader Lambda."
  type        = string
}

variable "quarantine_prefix" {
  description = "S3 quarantine prefix."
  type        = string
}

variable "alarm_actions" {
  description = "Optional CloudWatch alarm actions."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Base tags applied to supported resources."
  type        = map(string)
  default     = {}
}
