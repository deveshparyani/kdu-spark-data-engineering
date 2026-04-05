import re
import time

import boto3

redshift_data = boto3.client("redshift-data")

STAGING_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {staging_table} (
  date date,
  discount_code varchar(255),
  orders double precision,
  revenue double precision,
  show varchar(255),
  impressions double precision,
  sub_shipments double precision,
  new_orders double precision,
  lapsed_orders double precision,
  active_orders double precision,
  revenue_per_order double precision,
  revenue_per_impression double precision,
  impressions_per_order double precision,
  campaign_name varchar(255),
  campaign_item_id varchar(255),
  client_name varchar(255),
  file_type varchar(100),
  file_hash varchar(255),
  config_version varchar(50),
  ingest_date date,
  start_date date,
  end_date date,
  data_date date,
  curated_at timestamp
);
""".strip()


class RedshiftLoadError(Exception):
    pass


def lambda_handler(event, context):
    database = event["database"]
    workgroup_name = event["workgroup_name"]
    secret_arn = event["secret_arn"]
    target_table = event["target_table"]
    client = event["client"]
    copy_role_arn = event["copy_role_arn"]
    redshift_copy_source = event["redshift_copy_source"]
    staging_table = sanitize_identifier(event["staging_table_name"])

    create_stage_sql = STAGING_TABLE_SQL.format(staging_table=staging_table)
    copy_sql = (
        f"COPY {staging_table} FROM '{redshift_copy_source}' "
        f"IAM_ROLE '{copy_role_arn}' FORMAT AS PARQUET STATUPDATE ON;"
    )
    merge_sqls = [
        "BEGIN;",
        f"DELETE FROM {target_table} WHERE client = '{escape_sql_literal(client)}' "
        f"AND date IN (SELECT DISTINCT date FROM {staging_table});",
        (
            f"INSERT INTO {target_table} ("
            "date, discount_code, orders, revenue, show, impressions, sub_shipments, "
            "new_orders, lapsed_orders, active_orders, revenue_per_order, "
            "revenue_per_impression, impressions_per_order, campaign_name, "
            "campaign_item_id, client_name, client, file_type, file_hash, "
            "config_version, ingest_date, start_date, end_date, data_date, curated_at"
            f") SELECT date, discount_code, orders, revenue, show, impressions, "
            "sub_shipments, new_orders, lapsed_orders, active_orders, "
            "revenue_per_order, revenue_per_impression, impressions_per_order, "
            "campaign_name, campaign_item_id, client_name, "
            f"'{escape_sql_literal(client)}', file_type, file_hash, config_version, "
            "ingest_date, start_date, end_date, data_date, curated_at "
            f"FROM {staging_table};"
        ),
        "COMMIT;",
    ]
    drop_stage_sql = f"DROP TABLE IF EXISTS {staging_table};"

    create_stage_id = execute_statement(database, workgroup_name, secret_arn, create_stage_sql)
    wait_for_statement(create_stage_id)

    try:
        copy_stage_id = execute_statement(database, workgroup_name, secret_arn, copy_sql)
        wait_for_statement(copy_stage_id)

        merge_id = batch_execute_statement(database, workgroup_name, secret_arn, merge_sqls)
        wait_for_statement(merge_id)
    finally:
        try:
            drop_stage_id = execute_statement(database, workgroup_name, secret_arn, drop_stage_sql)
            wait_for_statement(drop_stage_id)
        except Exception as exc:  # pragma: no cover
            print(f"warning: failed to drop staging table {staging_table}: {exc}")

    return {
        "status": "success",
        "staging_table_name": staging_table,
        "target_table": target_table,
        "client": client,
        "redshift_copy_source": redshift_copy_source,
    }


def sanitize_identifier(identifier):
    safe_identifier = identifier.replace("-", "_")
    if not re.fullmatch(r"[A-Za-z0-9_.]+", safe_identifier):
        raise RedshiftLoadError(f"invalid_identifier:{identifier}")
    return safe_identifier


def escape_sql_literal(value):
    return value.replace("'", "''")


def execute_statement(database, workgroup_name, secret_arn, sql):
    response = redshift_data.execute_statement(
        Database=database,
        WorkgroupName=workgroup_name,
        SecretArn=secret_arn,
        Sql=sql,
    )
    print(f"execute_statement submitted: {response['Id']}")
    print(f"sql: {sql}")
    return response["Id"]


def batch_execute_statement(database, workgroup_name, secret_arn, sqls):
    response = redshift_data.batch_execute_statement(
        Database=database,
        WorkgroupName=workgroup_name,
        SecretArn=secret_arn,
        Sqls=sqls,
    )
    print(f"batch_execute_statement submitted: {response['Id']}")
    return response["Id"]


def wait_for_statement(statement_id, poll_seconds=5, max_wait_seconds=900):
    elapsed = 0

    while elapsed < max_wait_seconds:
        response = redshift_data.describe_statement(Id=statement_id)
        status = response["Status"]
        print(f"statement {statement_id} status: {status}")

        if status == "FINISHED":
            return response

        if status in {"FAILED", "ABORTED"}:
            error_message = response.get("Error", "Unknown Redshift error")
            raise RedshiftLoadError(
                f"Statement {statement_id} failed with status {status}: {error_message}"
            )

        time.sleep(poll_seconds)
        elapsed += poll_seconds

    raise RedshiftLoadError(
        f"Statement {statement_id} timed out after {max_wait_seconds} seconds"
    )
