variable "job_name" {
  description = "Glue job name."
  type        = string
}

variable "role_arn" {
  description = "IAM role ARN assumed by the Glue job."
  type        = string
}

variable "script_bucket" {
  description = "S3 bucket that stores Glue scripts."
  type        = string
}

variable "script_s3_key" {
  description = "S3 key for the Glue script."
  type        = string
}

variable "script_source_path" {
  description = "Local path to the Glue script that Terraform uploads."
  type        = string
}

variable "glue_version" {
  description = "AWS Glue version."
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 2
}

variable "timeout" {
  description = "Glue job timeout in minutes."
  type        = number
  default     = 60
}

variable "max_retries" {
  description = "Glue job retry count."
  type        = number
  default     = 1
}

variable "max_concurrent_runs" {
  description = "Maximum concurrent runs for the Glue job."
  type        = number
  default     = 1
}

variable "default_arguments" {
  description = "Glue default arguments."
  type        = map(string)
  default     = {}
}

variable "command_name" {
  description = "Glue job command name."
  type        = string
  default     = "glueetl"
}

variable "execution_class" {
  description = "Glue execution class."
  type        = string
  default     = "STANDARD"
}

variable "tags" {
  description = "Base tags applied to supported resources."
  type        = map(string)
  default     = {}
}
