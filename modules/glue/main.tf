resource "aws_s3_object" "script" {
  bucket = var.script_bucket
  key    = var.script_s3_key
  source = var.script_source_path
  etag   = filemd5(var.script_source_path)
  tags   = merge(var.tags, { Name = replace(var.job_name, "glue-job", "glue-script") })
}

resource "aws_glue_job" "this" {
  name              = var.job_name
  role_arn          = var.role_arn
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.timeout
  max_retries       = var.max_retries
  execution_class   = var.execution_class

  command {
    name            = var.command_name
    script_location = "s3://${var.script_bucket}/${var.script_s3_key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = var.max_concurrent_runs
  }

  default_arguments = merge(
    {
      "--enable-glue-datacatalog" = "true"
      "--job-language"            = "python"
    },
    var.default_arguments
  )

  tags = merge(var.tags, { Name = var.job_name })
}
