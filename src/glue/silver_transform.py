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
    "SOURCE_S3_URI",
    "CONFIG_S3_URI",
    "CONFIG_VERSION",
    "PROCESSED_S3_URI",
    "CLIENT",
    "FILE_TYPE",
    "FILE_HASH",
    "INGEST_DATE",
    "START_DATE",
    "END_DATE",
    "DATA_DATE",
]

ALLOWED_SOURCE_FORMATS = {"csv", "json", "parquet"}
METRIC_NAMESPACE = "KduSpark/GlueSilver"


class SilverJobError(Exception):
    pass


class ConfigShapeError(SilverJobError):
    pass


class JoinSourceError(SilverJobError):
    pass


class TransformationError(SilverJobError):
    pass


def parse_s3_uri(uri):
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def emit_log(event_type, **payload):
    message = {"event": event_type, **JOB_CONTEXT, **payload}
    print(json.dumps(message, default=str))


def publish_metric(metric_name, value, unit="Count"):
    cloudwatch.put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": metric_name,
                "Value": float(value),
                "Unit": unit,
                "Dimensions": [
                    {"Name": "Client", "Value": JOB_CONTEXT["client"]},
                    {"Name": "FileType", "Value": JOB_CONTEXT["file_type"]},
                    {"Name": "ConfigVersion", "Value": JOB_CONTEXT["config_version"]},
                ],
            }
        ],
    )


def dataframe_count(dataframe, stage_name):
    count = dataframe.count()
    emit_log("row_count", stage=stage_name, rows=count)
    return count


def read_config(config_uri):
    bucket, key = parse_s3_uri(config_uri)
    payload = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

    if key.endswith(".json"):
        config = json.loads(payload)
    elif key.endswith((".yaml", ".yml")):
        if yaml is None:
            raise ConfigShapeError("yaml_support_unavailable")
        config = yaml.safe_load(payload)
    else:
        raise ConfigShapeError(f"unsupported_config_format:{config_uri}")

    if not isinstance(config, dict):
        raise ConfigShapeError("config_must_be_object")
    return config


def load_dataset(spark, source_uri, file_format):
    format_name = (file_format or "csv").lower()
    if format_name not in ALLOWED_SOURCE_FORMATS:
        raise ConfigShapeError(f"unsupported_source_format:{format_name}")

    reader = spark.read
    if format_name == "csv":
        return reader.option("header", True).option("inferSchema", False).csv(source_uri)
    if format_name == "parquet":
        return reader.parquet(source_uri)
    return reader.json(source_uri)


def validate_config(config):
    source_format = config.get("source_format", "csv").lower()
    if source_format not in ALLOWED_SOURCE_FORMATS:
        raise ConfigShapeError(f"unsupported_source_format:{source_format}")

    rename_columns = config.get("rename_columns", {})
    if not isinstance(rename_columns, dict) or not all(
        isinstance(source, str) and isinstance(target, str)
        for source, target in rename_columns.items()
    ):
        raise ConfigShapeError("invalid_rename_columns")

    joins = config.get("joins", [])
    if not isinstance(joins, list):
        raise ConfigShapeError("invalid_joins")
    for join_spec in joins:
        if not isinstance(join_spec, dict):
            raise ConfigShapeError("invalid_join_spec")
        if not join_spec.get("path"):
            raise ConfigShapeError("missing_join_key:path")
        if "keys" not in join_spec:
            raise ConfigShapeError("missing_join_key:keys")
        join_keys = join_spec["keys"]
        if not isinstance(join_keys, (list, str)) or (isinstance(join_keys, list) and len(join_keys) == 0):
            raise ConfigShapeError("invalid_join_keys")
        column_mapping = join_spec.get("column_mapping", {})
        if column_mapping and (
            not isinstance(column_mapping, dict)
            or not all(isinstance(source, str) and isinstance(target, str) for source, target in column_mapping.items())
        ):
            raise ConfigShapeError("invalid_join_column_mapping")

    transformations = config.get("transformations", {})
    if not isinstance(transformations, dict):
        raise ConfigShapeError("invalid_transformations")

    derived_columns = transformations.get("derived_columns", [])
    if derived_columns and not isinstance(derived_columns, list):
        raise ConfigShapeError("invalid_derived_columns")
    for item in derived_columns:
        if not isinstance(item, dict) or "name" not in item or "expression" not in item:
            raise ConfigShapeError("invalid_derived_column_spec")

    cast_columns = transformations.get("cast_columns", {})
    if cast_columns and not isinstance(cast_columns, dict):
        raise ConfigShapeError("invalid_cast_columns")

    fill_nulls = transformations.get("fill_nulls", {})
    if fill_nulls and not isinstance(fill_nulls, dict):
        raise ConfigShapeError("invalid_fill_nulls")

    filter_expressions = transformations.get("filter_expressions", [])
    if filter_expressions and not isinstance(filter_expressions, list):
        raise ConfigShapeError("invalid_filter_expressions")


def apply_renames_and_drops(dataframe, config):
    for original_name, target_name in config.get("rename_columns", {}).items():
        if original_name in dataframe.columns:
            dataframe = dataframe.withColumnRenamed(original_name, target_name)

    drop_columns = [column for column in config.get("drop_columns", []) if column in dataframe.columns]
    if drop_columns:
        dataframe = dataframe.drop(*drop_columns)

    return dataframe


def apply_cleaning(dataframe, config):
    rules = config.get("cleaning_rules", {})
    if not isinstance(rules, dict):
        raise ConfigShapeError("invalid_cleaning_rules")

    for column_name, rule in rules.items():
        if column_name not in dataframe.columns:
            continue

        if rule == "trim":
            dataframe = dataframe.withColumn(column_name, F.trim(F.col(column_name)))
        elif rule == "lowercase":
            dataframe = dataframe.withColumn(column_name, F.lower(F.col(column_name)))
        elif rule == "uppercase":
            dataframe = dataframe.withColumn(column_name, F.upper(F.col(column_name)))
        elif rule == "date":
            dataframe = dataframe.withColumn(column_name, F.to_date(F.col(column_name)))
        elif rule == "timestamp":
            dataframe = dataframe.withColumn(column_name, F.to_timestamp(F.col(column_name)))
        else:
            raise ConfigShapeError(f"unsupported_cleaning_rule:{column_name}:{rule}")

    return dataframe


def apply_joins(dataframe, spark, config):
    joins = config.get("joins", [])
    quarantine_df = None

    for index, join_spec in enumerate(joins):
        join_name = join_spec["name"]
        emit_log("join_started", join_name=join_name, join_path=join_spec["path"])

        try:
            join_df = load_dataset(
                spark,
                join_spec["path"],
                join_spec.get("format", "csv")
            )
        except Exception as exc:  # pragma: no cover
            raise JoinSourceError(f"join_source_load_failed:{join_spec.get('path')}") from exc

        emit_log("join_source_columns_before_mapping", join_name=join_name, columns=join_df.columns)

        for source_column, target_column in join_spec.get("column_mapping", {}).items():
            if source_column in join_df.columns:
                join_df = join_df.withColumnRenamed(source_column, target_column)

        emit_log("join_source_columns_after_mapping", join_name=join_name, columns=join_df.columns)

        join_keys = join_spec["keys"]
        if isinstance(join_keys, str):
            join_keys = [join_keys]

        missing_left_keys = [column for column in join_keys if column not in dataframe.columns]
        if missing_left_keys:
            raise JoinSourceError(f"missing_left_join_keys:{join_name}:{','.join(sorted(missing_left_keys))}")

        missing_right_keys = [column for column in join_keys if column not in join_df.columns]
        if missing_right_keys:
            raise JoinSourceError(f"missing_right_join_keys:{join_name}:{','.join(sorted(missing_right_keys))}")

        normalization = join_spec.get("normalization", {})
        for column_name in join_keys:
            if normalization.get("lowercase"):
                dataframe = dataframe.withColumn(column_name, F.lower(F.col(column_name)))
                join_df = join_df.withColumn(column_name, F.lower(F.col(column_name)))

            if normalization.get("trim"):
                dataframe = dataframe.withColumn(column_name, F.trim(F.col(column_name)))
                join_df = join_df.withColumn(column_name, F.trim(F.col(column_name)))

        match_marker = f"__join_matched_{index}"
        join_df = join_df.withColumn(match_marker, F.lit(1))

        join_type = "left" if join_spec.get("quarantine_on_unmatched") else join_spec.get("how", "left")
        joined_df = dataframe.join(join_df, on=join_keys, how=join_type)

        if join_spec.get("quarantine_on_unmatched"):
            unmatched_condition = F.col(match_marker).isNull()
            for column_name in join_keys:
                unmatched_condition = unmatched_condition & F.col(column_name).isNotNull()

            unmatched = joined_df.filter(unmatched_condition).drop(match_marker).withColumn(
                "quarantine_reason",
                F.lit(join_spec.get("quarantine_reason", f"unmatched_{join_name}"))
            )

            unmatched_count = unmatched.count()
            emit_log("join_unmatched_rows", join_name=join_name, rows=unmatched_count)

            if unmatched_count > 0:
                quarantine_df = unmatched if quarantine_df is None else quarantine_df.unionByName(
                    unmatched,
                    allowMissingColumns=True,
                )

            joined_df = joined_df.filter(~unmatched_condition)

        dataframe = joined_df.drop(match_marker)
        emit_log("join_completed", join_name=join_name, rows=dataframe.count())

    return dataframe, quarantine_df


def apply_transformations(dataframe, config):
    transformations = config.get("transformations", {})

    for column_name, data_type in transformations.get("cast_columns", {}).items():
        if column_name in dataframe.columns:
            try:
                dataframe = dataframe.withColumn(column_name, F.col(column_name).cast(data_type))
            except Exception as exc:  # pragma: no cover
                raise TransformationError(f"cast_failed:{column_name}:{data_type}") from exc

    fill_nulls = {
        column_name: value for column_name, value in transformations.get("fill_nulls", {}).items()
        if column_name in dataframe.columns
    }
    if fill_nulls:
        dataframe = dataframe.fillna(fill_nulls)

    for derived in transformations.get("derived_columns", []):
        try:
            dataframe = dataframe.withColumn(derived["name"], F.expr(derived["expression"]))
        except Exception as exc:  # pragma: no cover
            raise TransformationError(f"derived_column_failed:{derived['name']}") from exc

    for expression in transformations.get("filter_expressions", []):
        try:
            dataframe = dataframe.filter(F.expr(expression))
        except Exception as exc:  # pragma: no cover
            raise TransformationError(f"filter_failed:{expression}") from exc

    return dataframe


def resolve_partition_date(dataframe, config):
    partition_column = config.get("partition_column", config.get("date_column", "date"))
    if partition_column in dataframe.columns:
        return dataframe.withColumn("partition_date", F.to_date(F.col(partition_column)))

    fallback_date = JOB_CONTEXT["data_date"] or JOB_CONTEXT["start_date"] or JOB_CONTEXT["ingest_date"]
    return dataframe.withColumn("partition_date", F.to_date(F.lit(fallback_date)))


def attach_metadata(dataframe):
    return (
        dataframe
        .withColumn("client", F.lit(JOB_CONTEXT["client"]))
        .withColumn("file_type", F.lit(JOB_CONTEXT["file_type"]))
        .withColumn("file_hash", F.lit(JOB_CONTEXT["file_hash"]))
        .withColumn("config_version", F.lit(JOB_CONTEXT["config_version"]))
        .withColumn("ingest_date", F.to_date(F.lit(JOB_CONTEXT["ingest_date"])))
        .withColumn("start_date", F.to_date(F.lit(JOB_CONTEXT["start_date"])))
        .withColumn("end_date", F.to_date(F.lit(JOB_CONTEXT["end_date"])))
        .withColumn("data_date", F.to_date(F.lit(JOB_CONTEXT["data_date"])))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("year", F.date_format(F.col("partition_date"), "yyyy"))
        .withColumn("month", F.date_format(F.col("partition_date"), "MM"))
        .withColumn("day", F.date_format(F.col("partition_date"), "dd"))
    )


def write_output(dataframe, target_uri):
    (
        dataframe.write.mode("overwrite")
        .format("parquet")
        .partitionBy("client", "year", "month", "day")
        .save(target_uri)
    )


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
}

s3_client = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
job_succeeded = False

try:
    config = read_config(args["CONFIG_S3_URI"])
    validate_config(config)

    emit_log("silver_job_started", source_s3_uri=args["SOURCE_S3_URI"], config_s3_uri=args["CONFIG_S3_URI"])

    dataframe = load_dataset(spark, args["SOURCE_S3_URI"], config.get("source_format", "csv"))
    input_rows = dataframe_count(dataframe, "load")
    publish_metric("input_rows", input_rows)

    dataframe = apply_renames_and_drops(dataframe, config)
    mapped_rows = dataframe_count(dataframe, "rename_drop")
    emit_log("mapping_complete", rows=mapped_rows, columns=dataframe.columns)

    dataframe = apply_cleaning(dataframe, config)
    post_cleaning_rows = dataframe_count(dataframe, "cleaning")
    publish_metric("post_cleaning_rows", post_cleaning_rows)

    dataframe, quarantine_df = apply_joins(dataframe, spark, config)
    post_join_rows = dataframe_count(dataframe, "join")
    publish_metric("post_join_rows", post_join_rows)

    dataframe = apply_transformations(dataframe, config)
    post_transform_rows = dataframe_count(dataframe, "transformation")
    publish_metric("post_transform_rows", post_transform_rows)

    dataframe = resolve_partition_date(dataframe, config)
    dataframe = attach_metadata(dataframe)

    output_rows = dataframe_count(dataframe, "final_write")
    quarantine_rows = 0
    quarantine_path = config.get("outputs", {}).get("quarantine_path")
    if quarantine_df is not None:
        quarantine_rows = quarantine_df.count()
        emit_log("quarantine_rows_pre_write", rows=quarantine_rows, quarantine_path=quarantine_path)
        if quarantine_rows > 0 and quarantine_path:
            quarantine_df.write.mode("overwrite").format("parquet").save(quarantine_path)
            emit_log("quarantine_written", rows=quarantine_rows, quarantine_path=quarantine_path)
    publish_metric("output_rows", output_rows)
    publish_metric("quarantine_rows", quarantine_rows)
    emit_log("quarantine_summary", quarantine_rows=quarantine_rows)

    write_output(dataframe, args["PROCESSED_S3_URI"])
    emit_log("silver_job_completed", output_rows=output_rows, target_uri=args["PROCESSED_S3_URI"])
    job_succeeded = True
except (ConfigShapeError, JoinSourceError, TransformationError) as exc:
    emit_log("silver_job_failed", error_type=exc.__class__.__name__, error_message=str(exc))
    raise
except Exception as exc:  # pragma: no cover
    emit_log("silver_job_failed", error_type="UnhandledSilverJobError", error_message=str(exc))
    raise
finally:
    if job_succeeded:
        job.commit()
