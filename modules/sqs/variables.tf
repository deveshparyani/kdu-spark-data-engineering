variable "queue_name" {
  description = "Primary SQS queue name."
  type        = string
}

variable "dlq_name" {
  description = "Dead-letter queue name."
  type        = string
}

variable "visibility_timeout_seconds" {
  description = "Visibility timeout for the primary queue."
  type        = number
  default     = 360
}

variable "message_retention_seconds" {
  description = "Message retention period for the primary queue."
  type        = number
  default     = 345600
}

variable "receive_wait_time_seconds" {
  description = "Long polling wait time for the queue."
  type        = number
  default     = 20
}

variable "max_receive_count" {
  description = "Maximum receives before moving a message to the DLQ."
  type        = number
  default     = 5
}

variable "alarm_actions" {
  description = "Optional CloudWatch alarm actions."
  type        = list(string)
  default     = []
}

variable "source_bucket_arn" {
  description = "Optional S3 bucket ARN allowed to publish events to the queue."
  type        = string
  default     = null
}

variable "enable_s3_publish_policy" {
  description = "Whether to create the S3-to-SQS publish policy for the queue."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Base tags applied to supported resources."
  type        = map(string)
  default     = {}
}
