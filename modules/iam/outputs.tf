output "lambda_upload_role_arn" {
  description = "ARN of the upload processor Lambda role."
  value       = aws_iam_role.lambda_upload.arn
}

output "lambda_adscribe_role_arn" {
  description = "ARN of the Adscribe Lambda role."
  value       = aws_iam_role.lambda_adscribe.arn
}

output "lambda_validator_role_arn" {
  description = "ARN of the config validator Lambda role."
  value       = aws_iam_role.lambda_validator.arn
}

output "lambda_redshift_loader_role_arn" {
  description = "ARN of the Redshift loader Lambda role."
  value       = aws_iam_role.lambda_redshift_loader.arn
}

output "glue_role_arn" {
  description = "ARN of the Glue execution role."
  value       = aws_iam_role.glue.arn
}

output "step_function_role_arn" {
  description = "ARN of the Step Functions role."
  value       = aws_iam_role.step_function.arn
}

output "redshift_copy_role_arn" {
  description = "ARN of the Redshift COPY role."
  value       = aws_iam_role.redshift_copy.arn
}
