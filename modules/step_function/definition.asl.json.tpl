{
  "Comment": "Validates config, acquires a distributed lock, runs silver and gold Glue jobs, and performs a per-run transactional Redshift load.",
  "StartAt": "ValidateConfig",
  "States": {
    "ValidateConfig": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${config_validator_lambda_arn}",
        "Payload": {
          "config_s3_uri.$": "$.config_s3_uri",
          "config_version.$": "$.config_version",
          "client.$": "$.metadata.client",
          "file_type.$": "$.metadata.file_type"
        }
      },
      "ResultSelector": {
        "config_version.$": "$.Payload.config_version",
        "config_format.$": "$.Payload.config_format",
        "defaults_applied.$": "$.Payload.defaults_applied",
        "normalized_config.$": "$.Payload.normalized_config"
      },
      "ResultPath": "$.config_validation",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "InvalidConfig"
        }
      ],
      "Next": "PrepareExecutionContext"
    },
    "PrepareExecutionContext": {
      "Type": "Pass",
      "Parameters": {
        "execution_id.$": "$$.Execution.Name",
        "staging_table_name.$": "States.Format('${redshift_staging_table}_{}', $$.Execution.Name)"
      },
      "ResultPath": "$.execution",
      "Next": "AcquireDistributedLock"
    },
    "AcquireDistributedLock": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:dynamodb:putItem",
      "Parameters": {
        "TableName": "${lock_table_name}",
        "Item": {
          "lock_key": {
            "S.$": "$.lock.lock_key"
          },
          "locked_by": {
            "S.$": "$.lock.locked_by"
          },
          "lock_expiry": {
            "N.$": "States.Format('{}', $.lock.lock_expiry_epoch)"
          },
          "created_at": {
            "S.$": "$.lock.created_at"
          }
        },
        "ConditionExpression": "attribute_not_exists(lock_key) OR lock_expiry < :now",
        "ExpressionAttributeValues": {
          ":now": {
            "N.$": "States.Format('{}', $.lock.current_epoch)"
          }
        }
      },
      "ResultPath": null,
      "Retry": [
        {
          "ErrorEquals": [
            "DynamoDB.ConditionalCheckFailedException"
          ],
          "IntervalSeconds": 30,
          "BackoffRate": 2,
          "MaxAttempts": 3
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "DynamoDB.ConditionalCheckFailedException"
          ],
          "Next": "LockUnavailable"
        },
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.failure",
          "Next": "PipelineFailed"
        }
      ],
      "Next": "RunSilverJob"
    },
    "RunSilverJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "${glue_silver_job_name}",
        "Arguments": {
          "--SOURCE_S3_URI.$": "States.Format('s3://{}/{}', $.bucket, $.object_key)",
          "--CONFIG_S3_URI.$": "$.config_s3_uri",
          "--CONFIG_VERSION.$": "$.config_validation.config_version",
          "--PROCESSED_S3_URI.$": "$.silver_output_path",
          "--CLIENT.$": "$.metadata.client",
          "--FILE_TYPE.$": "$.metadata.file_type",
          "--FILE_HASH.$": "$.file_hash",
          "--INGEST_DATE.$": "$.metadata.ingest_date",
          "--START_DATE.$": "$.metadata.start_date",
          "--END_DATE.$": "$.metadata.end_date",
          "--DATA_DATE.$": "$.metadata.data_date"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Glue.ConcurrentRunsExceededException",
            "Glue.OperationTimeoutException",
            "States.TaskFailed"
          ],
          "IntervalSeconds": 45,
          "BackoffRate": 2,
          "MaxAttempts": 6
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.failure",
          "Next": "ReleaseDistributedLockOnFailure"
        }
      ],
      "ResultPath": "$.silver_result",
      "Next": "RunGoldJob"
    },
    "RunGoldJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "${glue_gold_job_name}",
        "Arguments": {
          "--PROCESSED_S3_URI.$": "$.silver_output_path",
          "--CURATED_S3_URI.$": "$.gold_output_path",
          "--QUARANTINE_S3_URI.$": "$.quarantine_output_path",
          "--CONFIG_S3_URI.$": "$.config_s3_uri",
          "--CONFIG_VERSION.$": "$.config_validation.config_version",
          "--CLIENT.$": "$.metadata.client",
          "--FILE_TYPE.$": "$.metadata.file_type",
          "--FILE_HASH.$": "$.file_hash",
          "--INGEST_DATE.$": "$.metadata.ingest_date",
          "--START_DATE.$": "$.metadata.start_date",
          "--END_DATE.$": "$.metadata.end_date",
          "--DATA_DATE.$": "$.metadata.data_date",
          "--EXECUTION_ID.$": "$.execution.execution_id",
          "--STAGING_TABLE_NAME.$": "$.execution.staging_table_name"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Glue.ConcurrentRunsExceededException",
            "Glue.OperationTimeoutException",
            "States.TaskFailed"
          ],
          "IntervalSeconds": 45,
          "BackoffRate": 2,
          "MaxAttempts": 6
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.failure",
          "Next": "ReleaseDistributedLockOnFailure"
        }
      ],
      "ResultPath": "$.gold_result",
      "Next": "RunRedshiftLoad"
    },
    "RunRedshiftLoad": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${redshift_loader_lambda_arn}",
        "Payload": {
          "database": "${redshift_database}",
          "workgroup_name": "${redshift_workgroup_name}",
          "secret_arn": "${redshift_secret_arn}",
          "staging_table_name.$": "$.execution.staging_table_name",
          "target_table": "${redshift_target_table}",
          "redshift_copy_source.$": "$.redshift_copy_source",
          "copy_role_arn": "${copy_role_arn}",
          "client.$": "$.metadata.client"
        }
      },
      "ResultPath": "$.redshift_result",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "ResultPath": "$.failure",
          "Next": "ReleaseDistributedLockOnFailure"
        }
      ],
      "Next": "ReleaseDistributedLock"
    },
    "ReleaseDistributedLock": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:dynamodb:deleteItem",
      "Parameters": {
        "TableName": "${lock_table_name}",
        "Key": {
          "lock_key": {
            "S.$": "$.lock.lock_key"
          }
        }
      },
      "ResultPath": null,
      "Next": "Success"
    },
    "ReleaseDistributedLockOnFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:dynamodb:deleteItem",
      "Parameters": {
        "TableName": "${lock_table_name}",
        "Key": {
          "lock_key": {
            "S.$": "$.lock.lock_key"
          }
        }
      },
      "ResultPath": null,
      "Next": "PipelineFailed"
    },
    "InvalidConfig": {
      "Type": "Fail",
      "Cause": "Config validation failed."
    },
    "LockUnavailable": {
      "Type": "Fail",
      "Cause": "A distributed lock already exists for this client and date range."
    },
    "PipelineFailed": {
      "Type": "Fail",
      "Cause": "Pipeline processing failed."
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
