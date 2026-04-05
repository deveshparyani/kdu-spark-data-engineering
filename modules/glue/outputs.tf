output "job_name" {
  description = "Glue job name."
  value       = aws_glue_job.this.name
}

output "job_arn" {
  description = "Glue job ARN."
  value       = aws_glue_job.this.arn
}

output "script_s3_uri" {
  description = "S3 URI of the uploaded Glue script."
  value       = "s3://${var.script_bucket}/${var.script_s3_key}"
}
