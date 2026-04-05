variable "table_name" {
  description = "DynamoDB table name."
  type        = string
}

variable "hash_key_name" {
  description = "Partition key attribute name."
  type        = string
  default     = "file_hash"
}

variable "point_in_time_recovery" {
  description = "Whether point-in-time recovery is enabled."
  type        = bool
  default     = true
}

variable "ttl_attribute_name" {
  description = "Optional TTL attribute name."
  type        = string
  default     = null
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
