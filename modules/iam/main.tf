locals {
  state_machine_arn           = "arn:aws:states:${var.region}:${var.account_id}:stateMachine:${var.state_machine_name}"
  glue_job_arns               = [for name in var.glue_job_names : "arn:aws:glue:${var.region}:${var.account_id}:job/${name}"]
  adscribe_secret_arn_pattern = "arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:${var.adscribe_secret_name}*"
  redshift_secret_arn_pattern = "arn:aws:secretsmanager:${var.region}:${var.account_id}:secret:${var.redshift_secret_name}*"
  redshift_workgroup_arn      = "arn:aws:redshift-serverless:${var.region}:${var.account_id}:workgroup/${var.redshift_workgroup_name}"
  config_validator_lambda_arn = "arn:aws:lambda:${var.region}:${var.account_id}:function:${var.config_validator_lambda_name}"
  redshift_loader_lambda_arn  = "arn:aws:lambda:${var.region}:${var.account_id}:function:${var.redshift_loader_lambda_name}"
  upload_lambda_dlq_arn       = "arn:aws:sqs:${var.region}:${var.account_id}:${var.upload_lambda_name}-dlq"
  adscribe_lambda_dlq_arn     = "arn:aws:sqs:${var.region}:${var.account_id}:${var.adscribe_lambda_name}-dlq"
  validator_lambda_dlq_arn    = "arn:aws:sqs:${var.region}:${var.account_id}:${var.config_validator_lambda_name}-dlq"
  redshift_loader_dlq_arn     = "arn:aws:sqs:${var.region}:${var.account_id}:${var.redshift_loader_lambda_name}-dlq"
  raw_objects_arn             = "${var.bucket_arn}/${trim(var.raw_prefix, "/")}/*"
  processed_objects_arn       = "${var.bucket_arn}/${trim(var.processed_prefix, "/")}/*"
  curated_objects_arn         = "${var.bucket_arn}/${trim(var.curated_prefix, "/")}/*"
  quarantine_objects_arn      = "${var.bucket_arn}/${trim(var.quarantine_prefix, "/")}/*"
  config_objects_arn          = "${var.bucket_arn}/${trim(var.configs_prefix, "/")}/*"
  script_objects_arn          = "${var.bucket_arn}/${trim(var.scripts_prefix, "/")}/*"
  temp_objects_arn            = "${var.bucket_arn}/${trim(var.temp_prefix, "/")}/*"
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "step_functions_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "redshift_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_upload" {
  name               = var.lambda_upload_role_name
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = merge(var.tags, { Name = var.lambda_upload_role_name })
}

resource "aws_iam_role_policy" "lambda_upload" {
  name = "${var.lambda_upload_role_name}-policy"
  role = aws_iam_role.lambda_upload.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteLambdaLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.region}:${var.account_id}:*"
      },
      {
        Sid    = "ReadRawObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectTagging"
        ]
        Resource = [
          local.raw_objects_arn,
          local.config_objects_arn
        ]
      },
      {
        Sid      = "ListBucketPrefixes"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.bucket_arn
      },
      {
        Sid    = "UseIngestionTables"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem"
        ]
        Resource = [
          var.dynamodb_table_arn,
          var.readiness_table_arn
        ]
      },
      {
        Sid    = "ConsumeIngestionQueue"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ChangeMessageVisibility"
        ]
        Resource = var.ingestion_queue_arn
      },
      {
        Sid      = "StartPipelineStateMachine"
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = local.state_machine_arn
      },
      {
        Sid      = "SendToLambdaDlq"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = local.upload_lambda_dlq_arn
      },
      {
        Sid    = "PublishTracingData"
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "lambda_adscribe" {
  name               = var.lambda_adscribe_role_name
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = merge(var.tags, { Name = var.lambda_adscribe_role_name })
}

resource "aws_iam_role_policy" "lambda_adscribe" {
  name = "${var.lambda_adscribe_role_name}-policy"
  role = aws_iam_role.lambda_adscribe.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteLambdaLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.region}:${var.account_id}:*"
      },
      {
        Sid      = "GetAdscribeSecret"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = local.adscribe_secret_arn_pattern
      },
      {
        Sid    = "WriteRawObjects"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload"
        ]
        Resource = local.raw_objects_arn
      },
      {
        Sid      = "ListRawBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.bucket_arn
      },
      {
        Sid      = "SendToLambdaDlq"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = local.adscribe_lambda_dlq_arn
      },
      {
        Sid    = "PublishTracingData"
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "lambda_validator" {
  name               = var.lambda_validator_role_name
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = merge(var.tags, { Name = var.lambda_validator_role_name })
}

resource "aws_iam_role_policy" "lambda_validator" {
  name = "${var.lambda_validator_role_name}-policy"
  role = aws_iam_role.lambda_validator.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteLambdaLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.region}:${var.account_id}:*"
      },
      {
        Sid    = "ReadConfigObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectTagging"
        ]
        Resource = local.config_objects_arn
      },
      {
        Sid      = "ListConfigBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.bucket_arn
      },
      {
        Sid      = "SendToLambdaDlq"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = local.validator_lambda_dlq_arn
      },
      {
        Sid    = "PublishTracingData"
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "lambda_redshift_loader" {
  name               = var.lambda_redshift_loader_role_name
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = merge(var.tags, { Name = var.lambda_redshift_loader_role_name })
}

resource "aws_iam_role_policy" "lambda_redshift_loader" {
  name = "${var.lambda_redshift_loader_role_name}-policy"
  role = aws_iam_role.lambda_redshift_loader.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteLambdaLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.region}:${var.account_id}:*"
      },
      {
        Sid    = "ExecuteRedshiftStatements"
        Effect = "Allow"
        Action = [
          "redshift-data:BatchExecuteStatement",
          "redshift-data:ExecuteStatement",
          "redshift-data:DescribeStatement",
          "redshift-data:CancelStatement"
        ]
        Resource = "*"
      },
      {
        Sid      = "ReadRedshiftSecret"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = local.redshift_secret_arn_pattern
      },
      {
        Sid      = "SendToLambdaDlq"
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = local.redshift_loader_dlq_arn
      },
      {
        Sid    = "PublishTracingData"
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "glue" {
  name               = var.glue_role_name
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags               = merge(var.tags, { Name = var.glue_role_name })
}

resource "aws_iam_role_policy" "glue" {
  name = "${var.glue_role_name}-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowGlueLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.region}:${var.account_id}:*"
      },
      {
        Sid      = "ListDataLakeBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.bucket_arn
      },
      {
        Sid    = "ReadAndWriteDataLakeObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          local.raw_objects_arn,
          local.processed_objects_arn,
          local.curated_objects_arn,
          local.quarantine_objects_arn,
          local.config_objects_arn,
          local.script_objects_arn,
          local.temp_objects_arn
        ]
      },
      {
        Sid      = "PublishMetrics"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "step_function" {
  name               = var.step_function_role_name
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume_role.json
  tags               = merge(var.tags, { Name = var.step_function_role_name })
}

resource "aws_iam_role_policy" "step_function" {
  name = "${var.step_function_role_name}-policy"
  role = aws_iam_role.step_function.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteExecutionLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups",
          "logs:PutLogEvents",
          "logs:CreateLogStream",
          "logs:CreateLogGroup"
        ]
        Resource = "*"
      },
      {
        Sid      = "InvokeConfigValidator"
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = local.config_validator_lambda_arn
      },
      {
        Sid      = "InvokeRedshiftLoader"
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = local.redshift_loader_lambda_arn
      },
      {
        Sid    = "RunGlueJobs"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = local.glue_job_arns
      },
      {
        Sid    = "ExecuteRedshiftStatements"
        Effect = "Allow"
        Action = [
          "redshift-data:BatchExecuteStatement",
          "redshift-data:ExecuteStatement",
          "redshift-data:DescribeStatement",
          "redshift-data:CancelStatement"
        ]
        Resource = "*"
      },
      {
        Sid      = "ReadRedshiftSecret"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = local.redshift_secret_arn_pattern
      },
      {
        Sid    = "ManageDistributedLocks"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:DeleteItem",
          "dynamodb:GetItem"
        ]
        Resource = var.lock_table_arn
      }
    ]
  })
}

resource "aws_iam_role" "redshift_copy" {
  name               = var.redshift_copy_role_name
  assume_role_policy = data.aws_iam_policy_document.redshift_assume_role.json
  tags               = merge(var.tags, { Name = var.redshift_copy_role_name })
}

resource "aws_iam_role_policy" "redshift_copy" {
  name = "${var.redshift_copy_role_name}-policy"
  role = aws_iam_role.redshift_copy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListCuratedBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.bucket_arn
      },
      {
        Sid      = "ReadCuratedObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = local.curated_objects_arn
      }
    ]
  })
}
