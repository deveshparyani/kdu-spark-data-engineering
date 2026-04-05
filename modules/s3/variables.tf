variable "bucket_name" {
  description = "S3 bucket name."
  type        = string
}

variable "force_destroy" {
  description = "Whether to allow bucket destruction with objects present."
  type        = bool
  default     = false
}

variable "encryption_algorithm" {
  description = "Server-side encryption algorithm."
  type        = string
  default     = "AES256"
}

variable "kms_key_id" {
  description = "Optional KMS key ID/ARN when using aws:kms encryption."
  type        = string
  default     = null
}

variable "prefixes" {
  description = "Logical prefixes to initialize inside the bucket."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Base tags applied to supported resources."
  type        = map(string)
  default     = {}
}
