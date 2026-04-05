resource "aws_sqs_queue" "dlq" {
  name                      = var.dlq_name
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
  tags                      = merge(var.tags, { Name = var.dlq_name })
}

resource "aws_sqs_queue" "this" {
  name                       = var.queue_name
  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = var.message_retention_seconds
  receive_wait_time_seconds  = var.receive_wait_time_seconds
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })

  tags = merge(var.tags, { Name = var.queue_name })
}

data "aws_iam_policy_document" "allow_s3_publish" {
  count = var.enable_s3_publish_policy ? 1 : 0

  statement {
    sid    = "AllowS3EventNotifications"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.this.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [var.source_bucket_arn]
    }
  }
}

resource "aws_sqs_queue_policy" "allow_s3_publish" {
  count = var.enable_s3_publish_policy ? 1 : 0

  queue_url = aws_sqs_queue.this.id
  policy    = data.aws_iam_policy_document.allow_s3_publish[0].json
}

resource "aws_cloudwatch_metric_alarm" "oldest_message_age" {
  alarm_name          = "${var.queue_name}-oldest-message-age"
  alarm_description   = "Triggers when messages remain buffered in the ingestion queue for too long."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 300
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_actions

  dimensions = {
    QueueName = aws_sqs_queue.this.name
  }

  tags = merge(var.tags, { Name = "${var.queue_name}-oldest-message-age" })
}

resource "aws_cloudwatch_metric_alarm" "dlq_visible_messages" {
  alarm_name          = "${var.dlq_name}-visible-messages"
  alarm_description   = "Triggers when messages are moved to the SQS DLQ."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_actions

  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }

  tags = merge(var.tags, { Name = "${var.dlq_name}-visible-messages" })
}
