resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/vendedlogs/states/${var.name}"
  retention_in_days = var.log_retention_days
  tags              = merge(var.tags, { Name = "/aws/vendedlogs/states/${var.name}" })
}

resource "aws_sfn_state_machine" "this" {
  name     = var.name
  role_arn = var.role_arn

  definition = templatefile("${path.module}/definition.asl.json.tpl", {
    config_validator_lambda_arn = var.config_validator_lambda_arn
    glue_silver_job_name        = var.glue_silver_job_name
    glue_gold_job_name          = var.glue_gold_job_name
    lock_table_name             = var.lock_table_name
    redshift_workgroup_name     = var.redshift_workgroup_name
    redshift_database           = var.redshift_database
    redshift_secret_arn         = var.redshift_secret_arn
    redshift_target_table       = var.redshift_target_table
    redshift_staging_table      = var.redshift_staging_table
    copy_role_arn               = var.copy_role_arn
    redshift_loader_lambda_arn  = var.redshift_loader_lambda_arn
    quarantine_prefix           = var.quarantine_prefix
  })

  logging_configuration {
    include_execution_data = true
    level                  = "ALL"

    log_destination = "${aws_cloudwatch_log_group.this.arn}:*"
  }

  tracing_configuration {
    enabled = true
  }

  tags = merge(var.tags, { Name = var.name })
}

resource "aws_cloudwatch_metric_alarm" "failed_executions" {
  alarm_name          = "${var.name}-failed-executions"
  alarm_description   = "Triggers when the state machine reports failed executions."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_actions

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.this.arn
  }

  tags = merge(var.tags, { Name = "${var.name}-failed-executions" })
}
