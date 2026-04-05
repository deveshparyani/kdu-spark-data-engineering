data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  bucket_name = coalesce(
    var.data_lake_bucket_name_override,
    lower("${var.project_name}-s3-data-lake-${var.environment}-${data.aws_caller_identity.current.account_id}")
  )

  names = {
    data_lake_bucket      = local.bucket_name
    idempotency_table     = "${var.project_name}-dynamodb-idempotency-${var.environment}"
    readiness_table       = "${var.project_name}-dynamodb-readiness-${var.environment}"
    lock_table            = "${var.project_name}-lock-table"
    upload_lambda         = "${var.project_name}-lambda-upload-trigger-${var.environment}"
    adscribe_lambda       = "${var.project_name}-lambda-adscribe-ingestion-${var.environment}"
    config_validator      = "${var.project_name}-config-validator"
    redshift_loader       = "${var.project_name}-lambda-redshift-loader-${var.environment}"
    silver_glue_job       = "${var.project_name}-glue-job-silver-${var.environment}"
    gold_glue_job         = "${var.project_name}-glue-job-gold-${var.environment}"
    state_machine         = coalesce(var.step_function_name_override, "${var.project_name}-step-function-orchestrator-${var.environment}")
    schedule_rule         = "${var.project_name}-eventbridge-daily-${var.environment}"
    ingestion_queue       = "${var.project_name}-ingestion-queue"
    ingestion_queue_dlq   = "${var.project_name}-ingestion-queue-dlq"
    redshift_namespace    = "${var.project_name}-redshift-namespace-${var.environment}"
    redshift_workgroup    = "${var.project_name}-redshift-workgroup-${var.environment}"
    redshift_admin_secret = "${var.project_name}-redshift-admin-${var.environment}"
    adscribe_secret       = coalesce(var.adscribe_secret_name_override, "${var.project_name}-adscribe-api-${var.environment}")
    lambda_upload_role    = "${var.project_name}-iam-lambda-upload-${var.environment}"
    lambda_adscribe_role  = "${var.project_name}-iam-lambda-adscribe-${var.environment}"
    lambda_validator_role = "${var.project_name}-iam-lambda-config-validator-${var.environment}"
    lambda_loader_role    = "${var.project_name}-iam-lambda-redshift-loader-${var.environment}"
    glue_role             = "${var.project_name}-iam-glue-${var.environment}"
    step_function_role    = "${var.project_name}-iam-step-functions-${var.environment}"
    redshift_copy_role    = "${var.project_name}-iam-redshift-copy-${var.environment}"
    alerts_topic          = "${var.project_name}-sns-alerts-${var.environment}"
  }

  common_tags = merge(
    {
      Purpose = "kdu data engineer project"
    },
    var.additional_tags
  )
}

resource "aws_sns_topic" "alerts" {
  count = var.alert_email == "" ? 0 : 1

  name = local.names.alerts_topic
  tags = merge(local.common_tags, { Name = local.names.alerts_topic })
}

resource "aws_sns_topic_subscription" "alerts_email" {
  count = var.alert_email == "" ? 0 : 1

  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_secretsmanager_secret" "adscribe" {
  name                    = local.names.adscribe_secret
  recovery_window_in_days = 7
  tags                    = merge(local.common_tags, { Name = local.names.adscribe_secret })
}

resource "aws_secretsmanager_secret_version" "adscribe" {
  count = var.adscribe_secret_string == null ? 0 : 1

  secret_id     = aws_secretsmanager_secret.adscribe.id
  secret_string = var.adscribe_secret_string
}

module "s3" {
  source = "./modules/s3"

  bucket_name          = local.names.data_lake_bucket
  force_destroy        = var.force_destroy_data_lake
  encryption_algorithm = var.s3_encryption_algorithm
  kms_key_id           = var.s3_kms_key_id
  prefixes = [
    var.raw_prefix,
    var.processed_prefix,
    var.curated_prefix,
    var.quarantine_prefix,
    var.configs_prefix,
    var.scripts_prefix,
    var.temp_prefix,
  ]
  tags = local.common_tags
}

module "dynamodb" {
  source = "./modules/dynamodb"

  table_name             = local.names.idempotency_table
  hash_key_name          = "file_hash"
  alarm_actions          = var.alert_email == "" ? [] : [aws_sns_topic.alerts[0].arn]
  point_in_time_recovery = true
  tags                   = local.common_tags
}

module "readiness_table" {
  source = "./modules/dynamodb"

  table_name             = local.names.readiness_table
  hash_key_name          = "batch_key"
  alarm_actions          = var.alert_email == "" ? [] : [aws_sns_topic.alerts[0].arn]
  point_in_time_recovery = true
  tags                   = local.common_tags
}

module "lock_table" {
  source = "./modules/dynamodb"

  table_name             = local.names.lock_table
  hash_key_name          = "lock_key"
  ttl_attribute_name     = "lock_expiry"
  alarm_actions          = var.alert_email == "" ? [] : [aws_sns_topic.alerts[0].arn]
  point_in_time_recovery = true
  tags                   = local.common_tags
}

module "sqs_ingestion" {
  source = "./modules/sqs"

  queue_name                 = local.names.ingestion_queue
  dlq_name                   = local.names.ingestion_queue_dlq
  visibility_timeout_seconds = var.ingestion_queue_visibility_timeout_seconds
  max_receive_count          = var.ingestion_queue_max_receive_count
  enable_s3_publish_policy   = true
  source_bucket_arn          = module.s3.bucket_arn
  alarm_actions              = var.alert_email == "" ? [] : [aws_sns_topic.alerts[0].arn]
  tags                       = local.common_tags
}

module "iam" {
  source = "./modules/iam"

  project_name                     = var.project_name
  environment                      = var.environment
  region                           = data.aws_region.current.region
  account_id                       = data.aws_caller_identity.current.account_id
  bucket_name                      = module.s3.bucket_name
  bucket_arn                       = module.s3.bucket_arn
  raw_prefix                       = var.raw_prefix
  processed_prefix                 = var.processed_prefix
  curated_prefix                   = var.curated_prefix
  quarantine_prefix                = var.quarantine_prefix
  configs_prefix                   = var.configs_prefix
  scripts_prefix                   = var.scripts_prefix
  temp_prefix                      = var.temp_prefix
  dynamodb_table_arn               = module.dynamodb.table_arn
  readiness_table_arn              = module.readiness_table.table_arn
  lock_table_arn                   = module.lock_table.table_arn
  ingestion_queue_arn              = module.sqs_ingestion.queue_arn
  upload_lambda_name               = local.names.upload_lambda
  adscribe_lambda_name             = local.names.adscribe_lambda
  config_validator_lambda_name     = local.names.config_validator
  state_machine_name               = local.names.state_machine
  glue_job_names                   = [local.names.silver_glue_job, local.names.gold_glue_job]
  redshift_workgroup_name          = local.names.redshift_workgroup
  adscribe_secret_name             = local.names.adscribe_secret
  redshift_secret_name             = local.names.redshift_admin_secret
  lambda_upload_role_name          = local.names.lambda_upload_role
  lambda_adscribe_role_name        = local.names.lambda_adscribe_role
  lambda_validator_role_name       = local.names.lambda_validator_role
  lambda_redshift_loader_role_name = local.names.lambda_loader_role
  redshift_loader_lambda_name      = local.names.redshift_loader
  glue_role_name                   = local.names.glue_role
  step_function_role_name          = local.names.step_function_role
  redshift_copy_role_name          = local.names.redshift_copy_role
  tags                             = local.common_tags
}

module "redshift" {
  source = "./modules/redshift"

  namespace_name       = local.names.redshift_namespace
  workgroup_name       = local.names.redshift_workgroup
  database_name        = var.redshift_database
  admin_username       = var.redshift_admin_username
  admin_secret_name    = local.names.redshift_admin_secret
  base_capacity        = var.redshift_base_capacity
  publicly_accessible  = var.redshift_publicly_accessible
  enhanced_vpc_routing = var.redshift_enhanced_vpc_routing
  subnet_ids           = var.redshift_subnet_ids
  security_group_ids   = var.redshift_security_group_ids
  copy_role_arn        = module.iam.redshift_copy_role_arn
  tags                 = local.common_tags
}

module "glue_silver" {
  source = "./modules/glue"

  job_name            = local.names.silver_glue_job
  role_arn            = module.iam.glue_role_arn
  script_bucket       = module.s3.bucket_name
  script_s3_key       = "${trim(var.scripts_prefix, "/")}/silver_transform.py"
  script_source_path  = "${path.root}/src/glue/silver_transform.py"
  glue_version        = var.glue_version
  worker_type         = var.glue_worker_type
  number_of_workers   = var.silver_number_of_workers
  timeout             = var.glue_timeout_minutes
  max_retries         = var.glue_max_retries
  max_concurrent_runs = var.silver_max_concurrent_runs
  default_arguments = {
    "--enable-auto-scaling"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-observability-metrics"     = "true"
    "--TempDir"                          = "s3://${module.s3.bucket_name}/${trim(var.temp_prefix, "/")}/silver/"
  }
  tags = local.common_tags
}

module "glue_gold" {
  source = "./modules/glue"

  job_name            = local.names.gold_glue_job
  role_arn            = module.iam.glue_role_arn
  script_bucket       = module.s3.bucket_name
  script_s3_key       = "${trim(var.scripts_prefix, "/")}/gold_aggregate.py"
  script_source_path  = "${path.root}/src/glue/gold_aggregate.py"
  glue_version        = var.glue_version
  worker_type         = var.glue_worker_type
  number_of_workers   = var.gold_number_of_workers
  timeout             = var.glue_timeout_minutes
  max_retries         = var.glue_max_retries
  max_concurrent_runs = var.gold_max_concurrent_runs
  default_arguments = {
    "--enable-auto-scaling"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-observability-metrics"     = "true"
    "--TempDir"                          = "s3://${module.s3.bucket_name}/${trim(var.temp_prefix, "/")}/gold/"
  }
  tags = local.common_tags
}

module "lambda_config_validator" {
  source = "./modules/lambda"

  function_name                  = local.names.config_validator
  description                    = "Validates client configs before orchestration."
  source_dir                     = "${path.root}/src/lambda/config_validator"
  handler                        = "handler.lambda_handler"
  runtime                        = var.lambda_runtime
  role_arn                       = module.iam.lambda_validator_role_arn
  timeout                        = var.lambda_timeout_seconds
  memory_size                    = var.lambda_memory_size_mb
  architectures                  = var.lambda_architectures
  reserved_concurrent_executions = var.config_validator_reserved_concurrency
  log_retention_days             = var.log_retention_days
  alarm_actions                  = var.alert_email == "" ? [] : [aws_sns_topic.alerts[0].arn]
  environment_variables          = {}
  tags                           = local.common_tags
}

module "step_function" {
  source = "./modules/step_function"

  name                        = local.names.state_machine
  role_arn                    = module.iam.step_function_role_arn
  log_retention_days          = var.log_retention_days
  config_validator_lambda_arn = module.lambda_config_validator.function_arn
  glue_silver_job_name        = local.names.silver_glue_job
  glue_gold_job_name          = local.names.gold_glue_job
  lock_table_name             = module.lock_table.table_name
  redshift_workgroup_name     = local.names.redshift_workgroup
  redshift_database           = var.redshift_database
  redshift_secret_arn         = module.redshift.admin_secret_arn
  redshift_target_table       = var.default_redshift_table
  redshift_staging_table      = var.redshift_staging_table
  copy_role_arn               = module.iam.redshift_copy_role_arn
  redshift_loader_lambda_arn  = module.lambda_redshift_loader.function_arn
  quarantine_prefix           = var.quarantine_prefix
  alarm_actions               = var.alert_email == "" ? [] : [aws_sns_topic.alerts[0].arn]
  tags                        = local.common_tags
}

module "lambda_upload_processor" {
  source = "./modules/lambda"

  function_name                   = local.names.upload_lambda
  description                     = "Processes raw S3 uploads, enforces idempotency, and launches the orchestration workflow."
  source_dir                      = "${path.root}/src/lambda/upload_trigger"
  handler                         = "handler.lambda_handler"
  runtime                         = var.lambda_runtime
  role_arn                        = module.iam.lambda_upload_role_arn
  timeout                         = var.lambda_timeout_seconds
  memory_size                     = var.lambda_memory_size_mb
  architectures                   = var.lambda_architectures
  reserved_concurrent_executions  = var.upload_processor_reserved_concurrency
  log_retention_days              = var.log_retention_days
  alarm_actions                   = var.alert_email == "" ? [] : [aws_sns_topic.alerts[0].arn]
  enable_sqs_event_source         = true
  sqs_event_source_arn            = module.sqs_ingestion.queue_arn
  sqs_batch_size                  = var.ingestion_queue_batch_size
  sqs_max_batching_window_seconds = var.ingestion_queue_max_batching_window_seconds
  sqs_max_concurrency             = var.ingestion_queue_max_concurrency
  environment_variables = {
    IDEMPOTENCY_TABLE      = module.dynamodb.table_name
    READINESS_TABLE        = module.readiness_table.table_name
    STATE_MACHINE_ARN      = module.step_function.state_machine_arn
    RAW_PREFIX             = var.raw_prefix
    PROCESSED_PREFIX       = var.processed_prefix
    CURATED_PREFIX         = var.curated_prefix
    QUARANTINE_PREFIX      = var.quarantine_prefix
    CONFIGS_PREFIX         = var.configs_prefix
    DEFAULT_CONFIG_VERSION = var.default_config_version
    CONFIG_FILE_EXTENSION  = var.config_file_extension
    LOCK_TTL_SECONDS       = tostring(var.lock_ttl_seconds)
    DEFAULT_REDSHIFT_TABLE = var.default_redshift_table
  }
  tags = local.common_tags
}

module "lambda_adscribe_ingestion" {
  source = "./modules/lambda"

  function_name                  = local.names.adscribe_lambda
  description                    = "Runs on a schedule, downloads the latest Adscribe extract, and stores it in the raw S3 zone."
  source_dir                     = "${path.root}/src/lambda/adscribe_ingestion"
  handler                        = "handler.lambda_handler"
  runtime                        = var.lambda_runtime
  role_arn                       = module.iam.lambda_adscribe_role_arn
  timeout                        = var.lambda_timeout_seconds
  memory_size                    = var.lambda_memory_size_mb
  architectures                  = var.lambda_architectures
  reserved_concurrent_executions = var.adscribe_ingestion_reserved_concurrency
  log_retention_days             = var.log_retention_days
  alarm_actions                  = var.alert_email == "" ? [] : [aws_sns_topic.alerts[0].arn]
  environment_variables = {
    ADSCRIBE_API_URL    = var.adscribe_api_url
    ADSCRIBE_SECRET_ARN = aws_secretsmanager_secret.adscribe.arn
    RAW_BUCKET          = module.s3.bucket_name
    RAW_PREFIX          = var.raw_prefix
  }
  tags = local.common_tags
}

module "lambda_redshift_loader" {
  source = "./modules/lambda"

  function_name                  = local.names.redshift_loader
  description                    = "Creates Redshift staging tables, copies curated parquet, merges into the fact table, and drops the staging table."
  source_dir                     = "${path.root}/src/lambda/redshift_loader"
  handler                        = "handler.lambda_handler"
  runtime                        = var.lambda_runtime
  role_arn                       = module.iam.lambda_redshift_loader_role_arn
  timeout                        = 900
  memory_size                    = var.lambda_memory_size_mb
  architectures                  = var.lambda_architectures
  reserved_concurrent_executions = null
  log_retention_days             = var.log_retention_days
  alarm_actions                  = var.alert_email == "" ? [] : [aws_sns_topic.alerts[0].arn]
  environment_variables          = {}
  tags                           = local.common_tags
}

module "eventbridge" {
  source = "./modules/eventbridge"

  rule_name           = local.names.schedule_rule
  schedule_expression = var.adscribe_schedule_expression
  target_lambda_arn   = module.lambda_adscribe_ingestion.function_arn
  target_lambda_name  = local.names.adscribe_lambda
  input = {
    source = "eventbridge"
    job    = "daily-adscribe-ingestion"
  }
  tags = local.common_tags
}

resource "aws_s3_bucket_notification" "raw_uploads" {
  bucket = module.s3.bucket_id

  queue {
    queue_arn     = module.sqs_ingestion.queue_arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "${trim(var.raw_prefix, "/")}/"
  }

  depends_on = [module.sqs_ingestion]
}
