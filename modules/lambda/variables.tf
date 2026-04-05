variable "function_name" {
  description = "Lambda function name."
  type        = string
}

variable "description" {
  description = "Lambda function description."
  type        = string
  default     = null
}

variable "source_dir" {
  description = "Local directory containing Lambda source code."
  type        = string
}

variable "handler" {
  description = "Lambda handler."
  type        = string
}

variable "runtime" {
  description = "Lambda runtime."
  type        = string
}

variable "role_arn" {
  description = "IAM role ARN assumed by the Lambda function."
  type        = string
}

variable "timeout" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 300
}

variable "memory_size" {
  description = "Lambda memory size in MB."
  type        = number
  default     = 512
}

variable "architectures" {
  description = "Lambda CPU architectures."
  type        = list(string)
  default     = ["x86_64"]
}

variable "reserved_concurrent_executions" {
  description = "Optional reserved concurrency."
  type        = number
  default     = null
}

variable "environment_variables" {
  description = "Lambda environment variables."
  type        = map(string)
  default     = {}
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 30
}

variable "alarm_actions" {
  description = "Optional CloudWatch alarm actions."
  type        = list(string)
  default     = []
}

variable "sqs_event_source_arn" {
  description = "Optional SQS queue ARN used as an event source."
  type        = string
  default     = null
}

variable "enable_sqs_event_source" {
  description = "Whether to create the SQS event source mapping for this Lambda."
  type        = bool
  default     = false
}

variable "sqs_batch_size" {
  description = "Batch size for the SQS event source mapping."
  type        = number
  default     = 10
}

variable "sqs_max_batching_window_seconds" {
  description = "Maximum batching window in seconds for the SQS event source mapping."
  type        = number
  default     = 5
}

variable "sqs_max_concurrency" {
  description = "Optional maximum concurrency for the SQS event source mapping."
  type        = number
  default     = null
}

variable "tags" {
  description = "Base tags applied to supported resources."
  type        = map(string)
  default     = {}
}
