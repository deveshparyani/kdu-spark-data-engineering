variable "project_name" {
  description = "Global project prefix used in resource names."
  type        = string
  default     = "kdu-spark"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "ap-southeast-1"
}

variable "data_lake_bucket_name_override" {
  description = "Optional explicit S3 bucket name. Leave null to generate a unique bucket name."
  type        = string
  default     = null
}

variable "force_destroy_data_lake" {
  description = "Whether to allow Terraform to destroy the data lake bucket even when it contains objects."
  type        = bool
  default     = false
}

variable "raw_prefix" {
  description = "Prefix for raw-zone data."
  type        = string
  default     = "raw"
}

variable "processed_prefix" {
  description = "Prefix for processed silver-zone data."
  type        = string
  default     = "processed"
}

variable "curated_prefix" {
  description = "Prefix for curated gold-zone data."
  type        = string
  default     = "curated"
}

variable "quarantine_prefix" {
  description = "Prefix for quarantined data."
  type        = string
  default     = "quarantine"
}

variable "configs_prefix" {
  description = "Prefix for configuration files consumed by Glue jobs."
  type        = string
  default     = "configs"
}

variable "scripts_prefix" {
  description = "Prefix used to store Glue ETL scripts."
  type        = string
  default     = "artifacts/glue"
}

variable "temp_prefix" {
  description = "Prefix used by Glue for temporary files."
  type        = string
  default     = "tmp/glue"
}

variable "s3_encryption_algorithm" {
  description = "S3 server-side encryption algorithm."
  type        = string
  default     = "AES256"
}

variable "s3_kms_key_id" {
  description = "Optional KMS key ID/ARN for S3 encryption."
  type        = string
  default     = null
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 30
}

variable "lambda_runtime" {
  description = "Runtime used by both Lambda functions."
  type        = string
  default     = "python3.12"
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 300
}

variable "lambda_memory_size_mb" {
  description = "Lambda memory size in MB."
  type        = number
  default     = 512
}

variable "lambda_architectures" {
  description = "Lambda CPU architecture."
  type        = list(string)
  default     = ["x86_64"]
}

variable "upload_processor_reserved_concurrency" {
  description = "Optional reserved concurrency for the upload processor Lambda."
  type        = number
  default     = null
}

variable "ingestion_queue_visibility_timeout_seconds" {
  description = "Visibility timeout for the ingestion SQS queue."
  type        = number
  default     = 360
}

variable "ingestion_queue_batch_size" {
  description = "Batch size for the upload processor Lambda SQS trigger."
  type        = number
  default     = 10
}

variable "ingestion_queue_max_batching_window_seconds" {
  description = "Maximum batching window for the upload processor SQS trigger."
  type        = number
  default     = 10
}

variable "ingestion_queue_max_concurrency" {
  description = "Maximum concurrency for the upload processor SQS trigger."
  type        = number
  default     = 10
}

variable "ingestion_queue_max_receive_count" {
  description = "Maximum receives before a message is moved to the ingestion DLQ."
  type        = number
  default     = 5
}

variable "adscribe_ingestion_reserved_concurrency" {
  description = "Optional reserved concurrency for the Adscribe ingestion Lambda."
  type        = number
  default     = null
}

variable "config_validator_reserved_concurrency" {
  description = "Optional reserved concurrency for the config validator Lambda."
  type        = number
  default     = null
}

variable "adscribe_schedule_expression" {
  description = "EventBridge schedule expression for Adscribe ingestion."
  type        = string
  default     = "cron(0 2 * * ? *)"
}

variable "adscribe_api_url" {
  description = "Base URL or export endpoint for the Adscribe API."
  type        = string
}

variable "adscribe_secret_name_override" {
  description = "Optional explicit secret name for Adscribe credentials."
  type        = string
  default     = null
}

variable "adscribe_secret_string" {
  description = "Optional JSON string stored in Secrets Manager for Adscribe credentials."
  type        = string
  default     = null
  sensitive   = true
}

variable "glue_version" {
  description = "AWS Glue version for both jobs."
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "silver_number_of_workers" {
  description = "Number of workers for the silver Glue job."
  type        = number
  default     = 2
}

variable "gold_number_of_workers" {
  description = "Number of workers for the gold Glue job."
  type        = number
  default     = 2
}

variable "glue_timeout_minutes" {
  description = "Glue job timeout in minutes."
  type        = number
  default     = 60
}

variable "glue_max_retries" {
  description = "Glue job retries handled by the service."
  type        = number
  default     = 1
}

variable "silver_max_concurrent_runs" {
  description = "Maximum concurrent runs allowed for the silver Glue job."
  type        = number
  default     = 1
}

variable "gold_max_concurrent_runs" {
  description = "Maximum concurrent runs allowed for the gold Glue job."
  type        = number
  default     = 1
}

variable "step_function_name_override" {
  description = "Optional explicit Step Functions state machine name."
  type        = string
  default     = null
}

variable "default_config_version" {
  description = "Default config version resolved by the upload processor when no explicit version is present."
  type        = string
  default     = "v1"
}

variable "config_file_extension" {
  description = "Default config file extension used in S3 for client configs."
  type        = string
  default     = "json"
}

variable "lock_ttl_seconds" {
  description = "Number of seconds a distributed lock remains valid."
  type        = number
  default     = 900
}

variable "redshift_database" {
  description = "Primary Redshift database name."
  type        = string
  default     = "analytics"
}

variable "redshift_admin_username" {
  description = "Redshift admin username."
  type        = string
  default     = "kduadmin"
}

variable "redshift_base_capacity" {
  description = "Redshift Serverless base RPUs."
  type        = number
  default     = 32
}

variable "redshift_publicly_accessible" {
  description = "Whether the Redshift Serverless workgroup is publicly accessible."
  type        = bool
  default     = true
}

variable "redshift_enhanced_vpc_routing" {
  description = "Whether Redshift Serverless uses enhanced VPC routing."
  type        = bool
  default     = true
}

variable "redshift_subnet_ids" {
  description = "Optional list of subnet IDs for the Redshift Serverless workgroup."
  type        = list(string)
  default     = []
}

variable "redshift_security_group_ids" {
  description = "Optional list of security group IDs for the Redshift Serverless workgroup."
  type        = list(string)
  default     = []
}

variable "default_redshift_table" {
  description = "Default target table for curated data COPY operations."
  type        = string
  default     = "public.fact_metrics"
}

variable "redshift_staging_table" {
  description = "Base name prefix used for Redshift per-run staging tables."
  type        = string
  default     = "public.kdu_spark_staging"
}

variable "alert_email" {
  description = "Optional email address subscribed to infrastructure alarms."
  type        = string
  default     = ""
}

variable "additional_tags" {
  description = "Additional tags applied to all supported resources."
  type        = map(string)
  default     = {}
}
