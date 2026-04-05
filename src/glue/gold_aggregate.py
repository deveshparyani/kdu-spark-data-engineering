import json
import sys
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

REQUIRED_ARGS = [
    "JOB_NAME",
    "PROCESSED_S3_URI",
    "CURATED_S3_URI",
    "QUARANTINE_S3_URI",
    "CONFIG_S3_URI",
    "CONFIG_VERSION",
    "CLIENT",
    "FILE_TYPE",
    "FILE_HASH",
    "INGEST_DATE",
    "START_DATE",
    "END_DATE",
    "DATA_DATE",
    "EXECUTION_ID",
    "STAGING_TABLE_NAME",
]

DEDUP_KEYS = ["client", "date", "discount_code", "show"]
MAX_INVALID_RATIO = 0.10
SEGMENT_COLUMNS = ["new_orders", "lapsed_orders", "active_orders"]


class GoldJobError(Exception):
    pass


class DataQualityError(GoldJobError):
    pass


def parse_s3_uri(uri):
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def emit_log(event_type, **payload):
    message = {"event": event_type, **JOB_CONTEXT, **payload}
    print(json.dumps(message, default=str))


def read_config(config_uri):
    bucket, key = parse_s3_uri(config_uri)
    payload = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

    if key.endswith(".json"):
        config = json.loads(payload)
    elif key.endswith((".yaml", ".yml")):
        if yaml is None:
            raise GoldJobError("yaml_support_unavailable")
        config = yaml.safe_load(payload)
    else:
        config = {}

    return config if isinstance(config, dict) else {}


def ensure_columns(dataframe, required_columns):
    for column_name, data_type in required_columns.items():
        if column_name not in dataframe.columns:
            dataframe = dataframe.withColumn(column_name, F.lit(None).cast(data_type))
    return dataframe


def validate_aggregation_inputs(dataframe, group_by_columns, metrics):
    missing_group_columns = [column for column in group_by_columns if column not in dataframe.columns]
    if missing_group_columns:
        raise GoldJobError(f"missing_group_by_columns:{','.join(sorted(missing_group_columns))}")

    missing_metric_columns = sorted(
        {
            metric["column"]
            for metric in metrics
            if metric["column"] not in dataframe.columns
        }
    )
    if missing_metric_columns:
        raise GoldJobError(f"missing_metric_columns:{','.join(missing_metric_columns)}")


def build_aggregations(metrics):
    aggregations = []
    for metric in metrics:
        function_name = metric["function"].lower()
        column_name = metric["column"]
        alias_name = metric.get("alias", f"{function_name}_{column_name}")

        if function_name == "sum":
            aggregations.append(F.sum(F.col(column_name)).alias(alias_name))
        elif function_name == "avg":
            aggregations.append(F.avg(F.col(column_name)).alias(alias_name))
        elif function_name == "count":
            aggregations.append(F.count(F.col(column_name)).alias(alias_name))
        elif function_name == "max":
            aggregations.append(F.max(F.col(column_name)).alias(alias_name))
        elif function_name == "min":
            aggregations.append(F.min(F.col(column_name)).alias(alias_name))
        else:
            raise GoldJobError(f"unsupported_aggregation:{function_name}")

    return aggregations


def attach_metadata(dataframe):
    return (
        dataframe
        .withColumn("client", F.coalesce(F.col("client"), F.lit(JOB_CONTEXT["client"])))
        .withColumn("file_type", F.lit(JOB_CONTEXT["file_type"]))
        .withColumn("file_hash", F.lit(JOB_CONTEXT["file_hash"]))
        .withColumn("config_version", F.lit(JOB_CONTEXT["config_version"]))
        .withColumn("ingest_date", F.to_date(F.lit(JOB_CONTEXT["ingest_date"])))
        .withColumn("start_date", F.to_date(F.lit(JOB_CONTEXT["start_date"])))
        .withColumn("end_date", F.to_date(F.lit(JOB_CONTEXT["end_date"])))
        .withColumn("data_date", F.to_date(F.lit(JOB_CONTEXT["data_date"])))
        .withColumn("curated_at", F.current_timestamp())
    )


def add_partition_columns(dataframe):
    return (
        dataframe
        .withColumn("date", F.to_date(F.col("date")))
        .withColumn("year", F.date_format(F.col("date"), "yyyy"))
        .withColumn("month", F.date_format(F.col("date"), "MM"))
        .withColumn("day", F.date_format(F.col("date"), "dd"))
    )


def metric_aliases(metrics):
    aliases = []
    for metric in metrics:
        aliases.append(metric.get("alias", f"{metric['function'].lower()}_{metric['column']}"))
    return aliases


def derive_orders_if_needed(dataframe, aliases):
    if "orders" not in aliases and all(column in dataframe.columns for column in SEGMENT_COLUMNS):
        return dataframe.withColumn(
            "orders",
            F.coalesce(F.col("new_orders"), F.lit(0))
            + F.coalesce(F.col("lapsed_orders"), F.lit(0))
            + F.coalesce(F.col("active_orders"), F.lit(0))
        )
    return dataframe


def resolve_filter_column(dataframe):
    if "partition_date" in dataframe.columns:
        return "partition_date"
    if "date" in dataframe.columns:
        return "date"
    raise GoldJobError("missing_date_filter_column")


def quarantine_rows(dataframe, target_uri):
    if dataframe is None:
        return 0

    count = dataframe.count()
    if count == 0:
        return 0

    (
        dataframe.write.mode("overwrite")
        .format("parquet")
        .save(target_uri)
    )
    return count


def build_invalid_condition(dataframe, aliases):
    checks = [F.col("revenue").isNull()]

    if "orders" in dataframe.columns and ("orders" in aliases or all(column in dataframe.columns for column in SEGMENT_COLUMNS)):
        checks.append(F.col("orders") < 0)

    if all(column in aliases for column in SEGMENT_COLUMNS):
        checks.append(
            (
                F.coalesce(F.col("new_orders"), F.lit(0))
                + F.coalesce(F.col("lapsed_orders"), F.lit(0))
                + F.coalesce(F.col("active_orders"), F.lit(0))
            ) != F.coalesce(F.col("orders"), F.lit(0))
        )

    condition = checks[0]
    for check in checks[1:]:
        condition = condition | check
    return condition


def invalid_reason_expression(aliases):
    expr = F.when(F.col("revenue").isNull(), F.lit("revenue_null"))

    if "orders" in aliases or all(alias in aliases for alias in SEGMENT_COLUMNS):
        expr = expr.when(F.col("orders") < 0, F.lit("orders_negative"))

    if all(alias in aliases for alias in SEGMENT_COLUMNS):
        expr = expr.when(
            (
                F.coalesce(F.col("new_orders"), F.lit(0))
                + F.coalesce(F.col("lapsed_orders"), F.lit(0))
                + F.coalesce(F.col("active_orders"), F.lit(0))
            ) != F.coalesce(F.col("orders"), F.lit(0)),
            F.lit("order_breakdown_mismatch"),
        )

    return expr.otherwise(F.lit("data_quality_failure"))


args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

JOB_CONTEXT = {
    "job_name": args["JOB_NAME"],
    "client": args["CLIENT"],
    "file_type": args["FILE_TYPE"],
    "file_hash": args["FILE_HASH"],
    "config_version": args["CONFIG_VERSION"],
    "ingest_date": args["INGEST_DATE"],
    "start_date": args["START_DATE"],
    "end_date": args["END_DATE"],
    "data_date": args["DATA_DATE"],
    "execution_id": args["EXECUTION_ID"],
    "staging_table_name": args["STAGING_TABLE_NAME"],
}

s3_client = boto3.client("s3")

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

try:
    emit_log(
        "gold_job_started",
        processed_s3_uri=args["PROCESSED_S3_URI"],
        curated_s3_uri=args["CURATED_S3_URI"],
        quarantine_s3_uri=args["QUARANTINE_S3_URI"],
    )

    config = read_config(args["CONFIG_S3_URI"])
    gold_config = config.get("gold_aggregations", config.get("aggregation", {}))
    group_by_columns = gold_config.get("group_by", ["client", "date", "discount_code", "show"])
    metrics = gold_config.get(
        "metrics",
        [
            {"column": "revenue", "function": "sum", "alias": "revenue"},
            {"column": "orders", "function": "sum", "alias": "orders"},
            {"column": "new_orders", "function": "sum", "alias": "new_orders"},
            {"column": "lapsed_orders", "function": "sum", "alias": "lapsed_orders"},
            {"column": "active_orders", "function": "sum", "alias": "active_orders"},
        ],
    )

    dataframe = spark.read.parquet(args["PROCESSED_S3_URI"]).filter(F.col("client") == args["CLIENT"])
    filter_column = resolve_filter_column(dataframe)
    dataframe = dataframe.filter(
        (F.to_date(F.col(filter_column)) >= F.to_date(F.lit(args["START_DATE"])))
        & (F.to_date(F.col(filter_column)) <= F.to_date(F.lit(args["END_DATE"])))
    )

    pre_aggregate_rows = dataframe.count()
    emit_log("gold_input_rows", rows=pre_aggregate_rows)
    if pre_aggregate_rows == 0:
        raise GoldJobError("no_rows_for_requested_client_date_range")

    validate_aggregation_inputs(dataframe, group_by_columns, metrics)
    aggregations = build_aggregations(metrics)
    if aggregations:
        dataframe = dataframe.groupBy(*group_by_columns).agg(*aggregations)

    aliases = metric_aliases(metrics)

    dataframe = ensure_columns(
        dataframe,
        {
            "date": "date",
            "client": "string",
            "discount_code": "string",
            "show": "string",
            "orders": "double",
            "revenue": "double",
            "new_orders": "double",
            "lapsed_orders": "double",
            "active_orders": "double",
        },
    )
    dataframe = derive_orders_if_needed(dataframe, aliases)

    dataframe = attach_metadata(dataframe)
    dataframe = add_partition_columns(dataframe)

    total_rows = dataframe.count()
    emit_log("gold_aggregated_rows", rows=total_rows)

    duplicate_keys = dataframe.groupBy(*DEDUP_KEYS).count().filter(F.col("count") > 1).drop("count")
    duplicate_rows = (
        dataframe.join(duplicate_keys, on=DEDUP_KEYS, how="inner")
        .withColumn("dq_reason", F.lit("duplicate_business_key"))
    )

    deduplicated = dataframe.dropDuplicates(DEDUP_KEYS)
    invalid_condition = build_invalid_condition(deduplicated, aliases)
    invalid_rows = deduplicated.filter(invalid_condition).withColumn("dq_reason", invalid_reason_expression(aliases))

    quarantine_df = duplicate_rows.unionByName(invalid_rows, allowMissingColumns=True).dropDuplicates(
        DEDUP_KEYS + ["dq_reason"]
    )
    quarantine_count = quarantine_rows(quarantine_df, args["QUARANTINE_S3_URI"])

    valid_rows = deduplicated.filter(~invalid_condition)
    valid_rows = add_partition_columns(valid_rows)
    valid_count = valid_rows.count()

    invalid_ratio = 0.0 if total_rows == 0 else quarantine_count / float(total_rows)
    emit_log(
        "gold_data_quality_summary",
        total_rows=total_rows,
        valid_rows=valid_count,
        quarantine_rows=quarantine_count,
        invalid_ratio=invalid_ratio,
    )

    if invalid_ratio > MAX_INVALID_RATIO:
        raise DataQualityError(
            f"invalid_row_ratio_exceeded:{invalid_ratio:.4f}:threshold={MAX_INVALID_RATIO:.2f}"
        )

    (
        valid_rows.write.mode("overwrite")
        .format("parquet")
        .partitionBy("client", "year", "month", "day")
        .save(args["CURATED_S3_URI"])
    )

    emit_log("gold_job_completed", output_rows=valid_count)
    job.commit()
except Exception as exc:
    emit_log("gold_job_failed", error_type=exc.__class__.__name__, error_message=str(exc))
    raise
