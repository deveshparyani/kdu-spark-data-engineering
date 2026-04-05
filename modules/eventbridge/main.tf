resource "aws_cloudwatch_event_rule" "this" {
  name                = var.rule_name
  description         = "Daily trigger for the Adscribe ingestion Lambda."
  schedule_expression = var.schedule_expression
  tags                = merge(var.tags, { Name = var.rule_name })
}

resource "aws_cloudwatch_event_target" "this" {
  rule      = aws_cloudwatch_event_rule.this.name
  target_id = "${var.rule_name}-lambda"
  arn       = var.target_lambda_arn
  input     = length(var.input) == 0 ? null : jsonencode(var.input)

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 2
  }
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = var.target_lambda_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.this.arn
}
