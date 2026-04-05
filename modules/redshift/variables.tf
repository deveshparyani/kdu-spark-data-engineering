variable "namespace_name" {
  description = "Redshift Serverless namespace name."
  type        = string
}

variable "workgroup_name" {
  description = "Redshift Serverless workgroup name."
  type        = string
}

variable "database_name" {
  description = "Redshift database name."
  type        = string
}

variable "admin_username" {
  description = "Redshift admin username."
  type        = string
}

variable "admin_secret_name" {
  description = "Secrets Manager secret name for Redshift credentials."
  type        = string
}

variable "base_capacity" {
  description = "Base capacity in RPUs."
  type        = number
  default     = 16
}

variable "publicly_accessible" {
  description = "Whether the workgroup is publicly accessible."
  type        = bool
  default     = true
}

variable "enhanced_vpc_routing" {
  description = "Whether enhanced VPC routing is enabled."
  type        = bool
  default     = true
}

variable "subnet_ids" {
  description = "Optional subnet IDs."
  type        = list(string)
  default     = []
}

variable "security_group_ids" {
  description = "Optional security group IDs."
  type        = list(string)
  default     = []
}

variable "copy_role_arn" {
  description = "IAM role ARN used by Redshift COPY."
  type        = string
}

variable "tags" {
  description = "Base tags applied to supported resources."
  type        = map(string)
  default     = {}
}
