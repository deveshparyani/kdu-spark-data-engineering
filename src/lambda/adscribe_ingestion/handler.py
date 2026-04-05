import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

s3_client = boto3.client("s3")
secretsmanager = boto3.client("secretsmanager")

ADSCRIBE_API_URL = os.environ["ADSCRIBE_API_URL"]
ADSCRIBE_SECRET_ARN = os.environ["ADSCRIBE_SECRET_ARN"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw").strip("/")


def lambda_handler(event, context):
    credentials = get_secret_payload(ADSCRIBE_SECRET_ARN)
    data_date = datetime.now(timezone.utc).date().isoformat()
    export_metadata = request_export(credentials, data_date)
    download_url = export_metadata["download_url"]
    client_name = export_metadata.get("client", "adscribe")
    file_type = export_metadata.get("file_type", "adscribe_csv")
    object_key = (
        f"{RAW_PREFIX}/source=adscribe/client={client_name}/file_type={file_type}/"
        f"date={data_date}/adscribe-{data_date}.csv"
    )

    upload_download_to_s3(download_url, RAW_BUCKET, object_key)

    return {
        "status": "uploaded",
        "bucket": RAW_BUCKET,
        "key": object_key,
        "data_date": data_date,
        "event_source": event.get("source", "manual"),
    }


def get_secret_payload(secret_arn):
    response = secretsmanager.get_secret_value(SecretId=secret_arn)
    return json.loads(response["SecretString"])


def request_export(credentials, data_date):
    request_body = json.dumps(
        {
            "request_type": "daily_export",
            "start_date": data_date,
            "end_date": data_date,
            "data_date": data_date,
        }
    ).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {credentials.get('api_key', '')}",
    }

    request = urllib.request.Request(
        ADSCRIBE_API_URL,
        data=request_body,
        headers=headers,
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))

    return {
        "download_url": payload.get("download_url") or payload["presigned_url"],
        "client": payload.get("client", credentials.get("client", "adscribe")),
        "file_type": payload.get("file_type", "adscribe_csv"),
    }


def upload_download_to_s3(download_url, bucket, key):
    with urllib.request.urlopen(download_url, timeout=300) as response:
        s3_client.upload_fileobj(response, bucket, key)
