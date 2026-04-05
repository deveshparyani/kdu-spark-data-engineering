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
      "ResultPath": "$.silver_job",
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
          "Next": "ReleaseDistributedLockOnFailure"
        }
      ],
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
      "ResultPath": "$.gold_job",
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
          "Next": "ReleaseDistributedLockOnFailure"
        }
      ],
      "Next": "CreateStagingTable"
    },
    "CreateStagingTable": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement",
      "Parameters": {
        "Database": "${redshift_database}",
        "SecretArn": "${redshift_secret_arn}",
        "WorkgroupName": "${redshift_workgroup_name}",
        "Sql.$": "States.Format('CREATE TABLE IF NOT EXISTS {} (LIKE ${redshift_target_table});', $.execution.staging_table_name)"
      },
      "ResultPath": "$.create_stage",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "ReleaseDistributedLockOnFailure"
        }
      ],
      "Next": "WaitForCreateStage"
    },
    "WaitForCreateStage": {
      "Type": "Wait",
      "Seconds": 10,
      "Next": "CheckCreateStageStatus"
    },
    "CheckCreateStageStatus": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:describeStatement",
      "Parameters": {
        "Id.$": "$.create_stage.Id"
      },
      "ResultPath": "$.create_stage_status",
      "Next": "CreateStageCompleted?"
    },
    "CreateStageCompleted?": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.create_stage_status.Status",
          "StringEquals": "FINISHED",
          "Next": "CopyToStaging"
        },
        {
          "Variable": "$.create_stage_status.Status",
          "StringEquals": "FAILED",
          "Next": "DropStagingTableOnFailure"
        },
        {
          "Variable": "$.create_stage_status.Status",
          "StringEquals": "ABORTED",
          "Next": "DropStagingTableOnFailure"
        }
      ],
      "Default": "WaitForCreateStage"
    },
    "CopyToStaging": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement",
      "Parameters": {
        "Database": "${redshift_database}",
        "SecretArn": "${redshift_secret_arn}",
        "WorkgroupName": "${redshift_workgroup_name}",
        "Sql.$": "States.Format('COPY {} FROM \\'{}\\' IAM_ROLE \\'${copy_role_arn}\\' FORMAT AS PARQUET TIMEFORMAT \\'auto\\' STATUPDATE ON;', $.execution.staging_table_name, $.redshift_copy_source)"
      },
      "ResultPath": "$.copy_stage",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "DropStagingTableOnFailure"
        }
      ],
      "Next": "WaitForCopyStage"
    },
    "WaitForCopyStage": {
      "Type": "Wait",
      "Seconds": 20,
      "Next": "CheckCopyStageStatus"
    },
    "CheckCopyStageStatus": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:describeStatement",
      "Parameters": {
        "Id.$": "$.copy_stage.Id"
      },
      "ResultPath": "$.copy_stage_status",
      "Next": "CopyStageCompleted?"
    },
    "CopyStageCompleted?": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.copy_stage_status.Status",
          "StringEquals": "FINISHED",
          "Next": "MergeIntoFactTable"
        },
        {
          "Variable": "$.copy_stage_status.Status",
          "StringEquals": "FAILED",
          "Next": "DropStagingTableOnFailure"
        },
        {
          "Variable": "$.copy_stage_status.Status",
          "StringEquals": "ABORTED",
          "Next": "DropStagingTableOnFailure"
        }
      ],
      "Default": "WaitForCopyStage"
    },
    "MergeIntoFactTable": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:batchExecuteStatement",
      "Parameters": {
        "Database": "${redshift_database}",
        "SecretArn": "${redshift_secret_arn}",
        "WorkgroupName": "${redshift_workgroup_name}",
        "Sqls.$": "States.Array('BEGIN;', States.Format('DELETE FROM ${redshift_target_table} WHERE client = \\'{}\\' AND date BETWEEN \\'{}\\' AND \\'{}\\';', $.metadata.client, $.metadata.start_date, $.metadata.end_date), States.Format('INSERT INTO ${redshift_target_table} SELECT * FROM {} WHERE client = \\'{}\\' AND date BETWEEN \\'{}\\' AND \\'{}\\';', $.execution.staging_table_name, $.metadata.client, $.metadata.start_date, $.metadata.end_date), 'COMMIT;')"
      },
      "ResultPath": "$.redshift_merge",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "DropStagingTableOnFailure"
        }
      ],
      "Next": "WaitForMerge"
    },
    "WaitForMerge": {
      "Type": "Wait",
      "Seconds": 20,
      "Next": "CheckMergeStatus"
    },
    "CheckMergeStatus": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:describeStatement",
      "Parameters": {
        "Id.$": "$.redshift_merge.Id"
      },
      "ResultPath": "$.redshift_merge_status",
      "Next": "MergeCompleted?"
    },
    "MergeCompleted?": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.redshift_merge_status.Status",
          "StringEquals": "FINISHED",
          "Next": "DropStagingTable"
        },
        {
          "Variable": "$.redshift_merge_status.Status",
          "StringEquals": "FAILED",
          "Next": "DropStagingTableOnFailure"
        },
        {
          "Variable": "$.redshift_merge_status.Status",
          "StringEquals": "ABORTED",
          "Next": "DropStagingTableOnFailure"
        }
      ],
      "Default": "WaitForMerge"
    },
    "DropStagingTable": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement",
      "Parameters": {
        "Database": "${redshift_database}",
        "SecretArn": "${redshift_secret_arn}",
        "WorkgroupName": "${redshift_workgroup_name}",
        "Sql.$": "States.Format('DROP TABLE IF EXISTS {};', $.execution.staging_table_name)"
      },
      "ResultPath": "$.drop_stage",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "ReleaseDistributedLock"
        }
      ],
      "Next": "ReleaseDistributedLock"
    },
    "DropStagingTableOnFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement",
      "Parameters": {
        "Database": "${redshift_database}",
        "SecretArn": "${redshift_secret_arn}",
        "WorkgroupName": "${redshift_workgroup_name}",
        "Sql.$": "States.Format('DROP TABLE IF EXISTS {};', $.execution.staging_table_name)"
      },
      "ResultPath": "$.drop_stage_failure",
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "ReleaseDistributedLockOnFailure"
        }
      ],
      "Next": "ReleaseDistributedLockOnFailure"
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
