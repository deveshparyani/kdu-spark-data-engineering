# KDU Spark AWS Data Pipeline Terraform

This project provisions a production-grade AWS data pipeline with buffered ingestion, versioned configs, preflight config validation, two-file readiness gating for multi-file clients, distributed locking, partitioned Glue outputs, quarantine-based data quality checks, and transactional Redshift loading.

Default deployment region: `ap-southeast-1` (Singapore).

## Architecture

1. UI uploads and the scheduled Adscribe ingestion Lambda write files into `raw/` in the S3 data lake.
2. S3 sends `ObjectCreated` events to the ingestion SQS queue.
3. The upload processor Lambda consumes SQS batches, computes a file hash, records idempotency metadata in DynamoDB, and either starts the pipeline immediately for single-file clients or waits in the readiness table until both required files for the same `client + date` have arrived.
4. Step Functions validates the client config, acquires a distributed DynamoDB lock for the client/date range, runs the silver Glue job, runs the gold Glue job, invokes the Redshift loader Lambda, and releases the lock.
5. Glue writes parquet using Hive-style partitions: `client/year/month/day`.

## Key upgrades

- SQS buffering between S3 and Lambda adds backpressure control, retry isolation, and burst smoothing.
- Config versioning is now explicit through `config_version`, versioned S3 config paths, DynamoDB tracking, and output dataset columns.
- Alpha, Beta, and Gamma now use a readiness DynamoDB table so the second required file is what starts the workflow.
- Config validation now happens before lock acquisition and before any Glue job runs, so bad schema configs fail fast.
- Distributed locking uses a dedicated DynamoDB lock table with conditional writes and TTL.
- Redshift loads now use a dedicated loader Lambda that creates a per-run staging table, copies curated parquet, merges into the fact table, and drops the staging table.
- Glue jobs are capped with `max_concurrent_runs`, while Step Functions retries on concurrency exceptions.
- Gold data quality checks quarantine invalid rows, enforce a 10% invalid-row threshold, and logically deduplicate by `(client, date, discount_code, show)`.

## Concurrency handling

- S3 publishes to the ingestion queue instead of invoking Lambda directly.
- The upload processor Lambda reads from SQS with configurable batch size and maximum concurrency.
- The queue has its own DLQ to absorb poison messages and avoid retry storms.
- Each Lambda still has its own async DLQ and CloudWatch alarms.
- Glue concurrency is capped, and Step Functions retries transient concurrency failures rather than fan out uncontrollably.

## Locking mechanism

- Lock table name: `kdu-spark-lock-table`
- Partition key: `lock_key`
- Lock key format: `client#date` for single-day loads, otherwise `client#start_date#end_date`
- Step Functions acquires the lock with a conditional DynamoDB `PutItem`
- If the lock already exists and has not expired, the workflow retries and then exits
- The lock record includes `locked_by`, `lock_expiry`, and `created_at`
- TTL is enabled on `lock_expiry`

- Readiness table name: `kdu-spark-dynamodb-readiness-<env>`
- Readiness key format: `client#data_date`
- Multi-file clients:
  - Alpha: `orders + codes`
  - Beta: `sales + shows_and_codes`
  - Gamma: `sales + salesforce`

## Data consistency guarantees

- No data loss: SQS buffers S3 events and redrives failed messages to a DLQ.
- No duplicate processing: the upload processor uses DynamoDB conditional writes keyed by `file_hash`.
- No premature starts for multi-file clients: the readiness table blocks pipeline start until the complete file pair for a business date is present.
- No race conditions: Step Functions acquires a distributed lock before running Glue and Redshift work.
- Atomic Redshift updates: curated data is copied into a run-specific staging table first, then merged into the fact table through a transaction-style batch of SQL statements inside the Redshift loader Lambda.
- Better query pruning: silver and gold parquet outputs are partitioned by `client/year/month/day`.
- Bad data is surfaced early: the config validator stops malformed runs up front, and the gold job quarantines invalid records before the Redshift load.

## Repository layout

```text
.
├── env/
│   ├── dev/
│   └── prod/
├── modules/
│   ├── dynamodb/
│   ├── eventbridge/
│   ├── glue/
│   ├── iam/
│   ├── lambda/
│   ├── redshift/
│   ├── s3/
│   ├── sqs/
│   └── step_function/
├── src/
│   ├── glue/
│   └── lambda/
├── main.tf
├── outputs.tf
├── provider.tf
├── terraform.tfvars
└── variables.tf
```

## Important conventions

- Raw file paths should include metadata-friendly segments such as `client=beta`, `file_type=sales`, `start_date=2024-01-15`, `end_date=2024-01-15`, and optionally `config_version=v2`.
- Multi-file raw uploads should use `date=YYYY-MM-DD` so readiness gating can pair files deterministically.
- Adscribe writes single-day extracts using `date=YYYY-MM-DD`, and that date becomes the canonical business date through locking, Glue, and Redshift.
- Config paths follow the versioned layout `configs/client=<client>/<config_version>.json` by default.
- Silver and gold outputs are written under the zone root and partitioned by `client/year/month/day`.
- Quarantined records are written under `quarantine/client=<client>/year=<yyyy>/month=<mm>/day=<dd>/`.

## Deploy

```bash
terraform init
terraform fmt -recursive
terraform plan -var-file=env/dev/terraform.tfvars
terraform apply -var-file=env/dev/terraform.tfvars
```

For production:

```bash
terraform init
terraform plan -var-file=env/prod/terraform.tfvars
terraform apply -var-file=env/prod/terraform.tfvars
```

## Notes

- Terraform state is local in this repo right now; no S3 backend is required.
- If you do not want Terraform state to hold the Adscribe secret value, leave `adscribe_secret_string` unset and populate the secret manually after apply.
- For private Redshift Serverless deployments, provide `redshift_subnet_ids` and `redshift_security_group_ids`.
- The Step Functions implementation uses AWS SDK integrations for DynamoDB and Redshift Data API calls.
- The transactional Redshift load expects the target fact table named by `default_redshift_table` to already exist, because each run creates and drops a dedicated staging table.
- Glue Job 1 emits JSON structured logs plus CloudWatch custom metrics for row counts at each stage.
- Glue Job 2 quarantines invalid rows and fails the run if more than 10% of records fail data quality checks.
