import hashlib
import json
import os
import re
import urllib.parse
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client("s3")
dynamodb = boto3.client("dynamodb")
stepfunctions = boto3.client("stepfunctions")

TABLE_NAME = os.environ["IDEMPOTENCY_TABLE"]
READINESS_TABLE = os.environ["READINESS_TABLE"]
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw").strip("/")
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed").strip("/")
CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated").strip("/")
QUARANTINE_PREFIX = os.environ.get("QUARANTINE_PREFIX", "quarantine").strip("/")
CONFIGS_PREFIX = os.environ.get("CONFIGS_PREFIX", "configs").strip("/")
DEFAULT_CONFIG_VERSION = os.environ.get("DEFAULT_CONFIG_VERSION", "v1")
CONFIG_FILE_EXTENSION = os.environ.get("CONFIG_FILE_EXTENSION", "json").lstrip(".")
LOCK_TTL_SECONDS = int(os.environ.get("LOCK_TTL_SECONDS", "900"))
BATCH_TIMEOUT_SECONDS = int(os.environ.get("BATCH_TIMEOUT_SECONDS", "86400"))
DEFAULT_REDSHIFT_TABLE = os.environ.get("DEFAULT_REDSHIFT_TABLE", "spark.fact_metrics")

REQUIRED_FILE_TYPES_BY_CLIENT = {
    "alpha": ["orders", "codes"],
    "beta": ["sales", "shows_and_codes"],
    "gamma": ["sales", "salesforce"],
}

PRIMARY_FILE_TYPE_BY_CLIENT = {
    "alpha": "orders",
    "beta": "sales",
    "gamma": "sales",
}


def lambda_handler(event, context):
    batch_item_failures = []
    responses = []

    for sqs_record in event.get("Records", []):
        try:
            if "body" in sqs_record:
                s3_event = json.loads(sqs_record["body"])
                records = s3_event.get("Records", [])
            else:
                records = [sqs_record]

            for s3_record in records:
                response = process_s3_record(s3_record)
                if response is not None:
                    responses.append(response)
        except Exception:
            batch_item_failures.append(
                {"itemIdentifier": sqs_record.get("messageId", "unknown")}
            )

    return {
        "results": responses,
        "batchItemFailures": batch_item_failures,
    }


def process_s3_record(record):
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    if not key.startswith(f"{RAW_PREFIX}/") or key.endswith("/"):
        return {"bucket": bucket, "key": key, "status": "ignored"}

    metadata = extract_metadata(key)
    file_hash = hash_s3_object(bucket, key)

    created = create_idempotency_record(file_hash, metadata)
    if not created:
        return {
            "bucket": bucket,
            "key": key,
            "file_hash": file_hash,
            "status": "skipped",
            "reason": "duplicate-file-hash",
        }

    client = metadata["client"]
    required_file_types = REQUIRED_FILE_TYPES_BY_CLIENT.get(client)

    if not required_file_types:
        execution_input = build_execution_input(bucket, key, file_hash, metadata)
        return start_pipeline(execution_input, metadata, bucket, key)

    batch_key = build_batch_key(metadata)
    register_batch_file(bucket, key, file_hash, metadata)

    batch_item = get_batch_item(batch_key)

    if not batch_is_complete(batch_item, required_file_types):
        update_idempotency_status(file_hash, "WAITING_FOR_BATCH")
        return {
            "bucket": bucket,
            "key": key,
            "file_hash": file_hash,
            "status": "pending-second-file",
            "batch_key": batch_key,
            "required_file_types": required_file_types,
            "received_file_types": sorted(list(batch_item.get("files", {}).keys())),
        }

    started = mark_batch_started(batch_key)
    if not started:
        return {
            "bucket": bucket,
            "key": key,
            "file_hash": file_hash,
            "status": "already-started",
            "batch_key": batch_key,
        }

    execution_input = build_execution_input_for_batch(batch_item)

    for file_info in batch_item.get("files", {}).values():
        update_idempotency_status(file_info["file_hash"], "PROCESSING")

    return start_pipeline(
        execution_input,
        execution_input["metadata"],
        execution_input["bucket"],
        execution_input["object_key"],
    )


def register_batch_file(bucket, key, file_hash, metadata):
    now = datetime.now(timezone.utc)
    batch_key = build_batch_key(metadata)
    client = metadata["client"]
    file_type = metadata["file_type"]

    try:
        dynamodb.put_item(
            TableName=READINESS_TABLE,
            Item={
                "batch_key": {"S": batch_key},
                "client": {"S": client},
                "data_date": {"S": metadata["data_date"]},
                "status": {"S": "PENDING"},
                "primary_file_type": {"S": PRIMARY_FILE_TYPE_BY_CLIENT[client]},
                "created_at": {"S": now.isoformat()},
                "updated_at": {"S": now.isoformat()},
                "timeout_at": {
                    "N": str(int((now + timedelta(seconds=BATCH_TIMEOUT_SECONDS)).timestamp()))
                },
                "files": {
                    "M": {
                        file_type: {
                            "M": build_file_record(bucket, key, file_hash, file_type, now)
                        }
                    }
                },
            },
            ConditionExpression="attribute_not_exists(batch_key)",
        )
        return
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise

    dynamodb.update_item(
        TableName=READINESS_TABLE,
        Key={"batch_key": {"S": batch_key}},
        UpdateExpression="SET #updated_at = :updated_at, #files.#file_type = :file_record",
        ExpressionAttributeNames={
            "#updated_at": "updated_at",
            "#files": "files",
            "#file_type": file_type,
        },
        ExpressionAttributeValues={
            ":updated_at": {"S": now.isoformat()},
            ":file_record": {"M": build_file_record(bucket, key, file_hash, file_type, now)},
        },
    )


def build_file_record(bucket, key, file_hash, file_type, timestamp):
    return {
        "bucket": {"S": bucket},
        "key": {"S": key},
        "s3_uri": {"S": f"s3://{bucket}/{key}"},
        "file_hash": {"S": file_hash},
        "file_type": {"S": file_type},
        "received_at": {"S": timestamp.isoformat()},
    }


def get_batch_item(batch_key):
    response = dynamodb.get_item(
        TableName=READINESS_TABLE,
        Key={"batch_key": {"S": batch_key}},
    )
    item = response.get("Item")
    if not item:
        raise ValueError(f"batch_not_found:{batch_key}")

    return deserialize_batch_item(item)


def deserialize_batch_item(item):
    files = {}
    for file_type, file_info in item.get("files", {}).get("M", {}).items():
        mapped = {}
        for key, value in file_info.get("M", {}).items():
            if "S" in value:
                mapped[key] = value["S"]
            elif "N" in value:
                mapped[key] = value["N"]
        files[file_type] = mapped

    return {
        "batch_key": item["batch_key"]["S"],
        "client": item["client"]["S"],
        "data_date": item["data_date"]["S"],
        "status": item["status"]["S"],
        "primary_file_type": item["primary_file_type"]["S"],
        "files": files,
    }


def batch_is_complete(batch_item, required_file_types):
    files = batch_item.get("files", {})
    return all(file_type in files for file_type in required_file_types)


def mark_batch_started(batch_key):
    try:
        dynamodb.update_item(
            TableName=READINESS_TABLE,
            Key={"batch_key": {"S": batch_key}},
            UpdateExpression="SET #status = :started, #started_at = :started_at",
            ConditionExpression="attribute_exists(batch_key) AND #status = :pending",
            ExpressionAttributeNames={
                "#status": "status",
                "#started_at": "started_at",
            },
            ExpressionAttributeValues={
                ":started": {"S": "STARTED"},
                ":pending": {"S": "PENDING"},
                ":started_at": {"S": datetime.now(timezone.utc).isoformat()},
            },
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def build_execution_input_for_batch(batch_item):
    client = batch_item["client"]
    primary_file_type = batch_item["primary_file_type"]
    primary_file = batch_item["files"][primary_file_type]
    primary_metadata = extract_metadata(primary_file["key"])

    execution_input = build_execution_input(
        primary_file["bucket"],
        primary_file["key"],
        primary_file["file_hash"],
        primary_metadata,
    )

    lookup_file_type = next(
        file_type
        for file_type in REQUIRED_FILE_TYPES_BY_CLIENT[client]
        if file_type != primary_file_type
    )
    lookup_file = batch_item["files"][lookup_file_type]

    execution_input["batch_key"] = batch_item["batch_key"]
    execution_input["required_file_types"] = REQUIRED_FILE_TYPES_BY_CLIENT[client]
    execution_input["files"] = {
        "primary": primary_file,
        "lookup": lookup_file,
    }
    execution_input["file_hashes"] = {
        file_type: info["file_hash"]
        for file_type, info in batch_item["files"].items()
    }

    return execution_input


def start_pipeline(execution_input, metadata, bucket, key):
    try:
        response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=build_execution_name(metadata["client"], execution_input["file_hash"]),
            input=json.dumps(execution_input),
        )
        update_idempotency_status(execution_input["file_hash"], "PROCESSING")
    except ClientError:
        update_idempotency_status(execution_input["file_hash"], "FAILED")
        raise

    return {
        "bucket": bucket,
        "key": key,
        "file_hash": execution_input["file_hash"],
        "status": "started",
        "execution_arn": response["executionArn"],
        "config_version": metadata["config_version"],
        "lock_key": execution_input["lock"]["lock_key"],
        "data_date": metadata["data_date"],
    }


def extract_metadata(key):
    parts = key.strip("/").split("/")
    default_date = datetime.now(timezone.utc).date().isoformat()
    attributes = {
        "client": "unknown",
        "file_type": "unknown",
        "ingest_date": default_date,
        "start_date": default_date,
        "end_date": default_date,
        "data_date": default_date,
        "config_version": DEFAULT_CONFIG_VERSION,
    }

    for part in parts:
        if "=" not in part:
            continue

        name, value = part.split("=", 1)
        if name in {"client", "file_type", "ingest_date", "start_date", "end_date", "config_version"}:
            attributes[name] = value
        elif name == "date":
            attributes["data_date"] = value
            attributes["start_date"] = value
            attributes["end_date"] = value

    if not attributes["start_date"]:
        attributes["start_date"] = attributes["data_date"]
    if not attributes["end_date"]:
        attributes["end_date"] = attributes["start_date"]
    if not attributes["data_date"]:
        attributes["data_date"] = attributes["start_date"]

    attributes["config_key"] = resolve_config_key(attributes["client"], attributes["config_version"])
    attributes["date_range"] = f"{attributes['start_date']}:{attributes['end_date']}"
    return attributes


def resolve_config_key(client, config_version):
    filename = config_version if "." in config_version else f"{config_version}.{CONFIG_FILE_EXTENSION}"
    return f"{CONFIGS_PREFIX}/client={client}/{filename}"


def build_batch_key(metadata):
    return f"{metadata['client']}#{metadata['data_date']}"


def build_execution_name(client, file_hash):
    safe_client = re.sub(r"[^A-Za-z0-9_]", "_", client)
    timestamp = int(datetime.now(timezone.utc).timestamp())
    return f"{safe_client}_{file_hash[:8]}_{timestamp}"


def hash_s3_object(bucket, key):
    hasher = hashlib.sha256()
    response = s3_client.get_object(Bucket=bucket, Key=key)
    stream = response["Body"]

    while True:
        chunk = stream.read(1024 * 1024)
        if not chunk:
            break
        hasher.update(chunk)

    return hasher.hexdigest()


def create_idempotency_record(file_hash, metadata):
    now = datetime.now(timezone.utc).isoformat()

    try:
        dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                "file_hash": {"S": file_hash},
                "client": {"S": metadata["client"]},
                "file_type": {"S": metadata["file_type"]},
                "date_range": {"S": metadata["date_range"]},
                "data_date": {"S": metadata["data_date"]},
                "config_version": {"S": metadata["config_version"]},
                "status": {"S": "RECEIVED"},
                "created_at": {"S": now},
            },
            ConditionExpression="attribute_not_exists(file_hash)",
        )
        return True
    except dynamodb.exceptions.ConditionalCheckFailedException:
        return False


def update_idempotency_status(file_hash, status):
    dynamodb.update_item(
        TableName=TABLE_NAME,
        Key={"file_hash": {"S": file_hash}},
        UpdateExpression="SET #status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": {"S": status}},
    )


def build_execution_input(bucket, key, file_hash, metadata):
    now = datetime.now(timezone.utc)
    lock_expiry = now + timedelta(seconds=LOCK_TTL_SECONDS)
    lock_key = build_lock_key(metadata)

    return {
        "bucket": bucket,
        "object_key": key,
        "file_name": key.rsplit("/", 1)[-1],
        "file_hash": file_hash,
        "config_version": metadata["config_version"],
        "config_s3_uri": f"s3://{bucket}/{metadata['config_key']}",
        "silver_output_path": f"s3://{bucket}/{PROCESSED_PREFIX}/",
        "gold_output_path": f"s3://{bucket}/{CURATED_PREFIX}/",
        "quarantine_output_path": build_quarantine_path(bucket, metadata),
        "redshift_table": DEFAULT_REDSHIFT_TABLE,
        "redshift_copy_source": build_redshift_copy_source(bucket, metadata),
        "lock": {
            "lock_key": lock_key,
            "locked_by": file_hash,
            "current_epoch": int(now.timestamp()),
            "lock_expiry_epoch": int(lock_expiry.timestamp()),
            "created_at": now.isoformat(),
        },
        "metadata": metadata,
    }


def build_lock_key(metadata):
    if metadata["start_date"] == metadata["end_date"]:
        return f"{metadata['client']}#{metadata['data_date']}"
    return f"{metadata['client']}#{metadata['start_date']}#{metadata['end_date']}"


def build_redshift_copy_source(bucket, metadata):
    return f"s3://{bucket}/{CURATED_PREFIX}/client={metadata['client']}/"


def build_quarantine_path(bucket, metadata):
    date_value = datetime.fromisoformat(metadata["data_date"])
    return (
        f"s3://{bucket}/{QUARANTINE_PREFIX}/client={metadata['client']}/"
        f"year={date_value:%Y}/month={date_value:%m}/day={date_value:%d}/"
    )
