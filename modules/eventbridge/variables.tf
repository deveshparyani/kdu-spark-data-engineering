variable "rule_name" {
  description = "EventBridge rule name."
  type        = string
}

variable "schedule_expression" {
  description = "EventBridge schedule expression."
  type        = string
}

variable "target_lambda_arn" {
  description = "ARN of the target Lambda function."
  type        = string
}

variable "target_lambda_name" {
  description = "Name of the target Lambda function."
  type        = string
}

variable "input" {
  description = "Optional JSON payload sent to the Lambda target."
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Base tags applied to supported resources."
  type        = map(string)
  default     = {}
}
