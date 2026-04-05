resource "aws_dynamodb_table" "this" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = var.hash_key_name

  attribute {
    name = var.hash_key_name
    type = "S"
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery
  }

  server_side_encryption {
    enabled = true
  }

  dynamic "ttl" {
    for_each = var.ttl_attribute_name == null ? [] : [var.ttl_attribute_name]

    content {
      attribute_name = ttl.value
      enabled        = true
    }
  }

  tags = merge(var.tags, { Name = var.table_name })
}

resource "aws_cloudwatch_metric_alarm" "read_throttle" {
  alarm_name          = "${var.table_name}-read-throttle"
  alarm_description   = "Triggers when DynamoDB read operations are throttled."
  namespace           = "AWS/DynamoDB"
  metric_name         = "ReadThrottleEvents"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_actions

  dimensions = {
    TableName = aws_dynamodb_table.this.name
  }

  tags = merge(var.tags, { Name = "${var.table_name}-read-throttle" })
}

resource "aws_cloudwatch_metric_alarm" "write_throttle" {
  alarm_name          = "${var.table_name}-write-throttle"
  alarm_description   = "Triggers when DynamoDB write operations are throttled."
  namespace           = "AWS/DynamoDB"
  metric_name         = "WriteThrottleEvents"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_actions

  dimensions = {
    TableName = aws_dynamodb_table.this.name
  }

  tags = merge(var.tags, { Name = "${var.table_name}-write-throttle" })
}
