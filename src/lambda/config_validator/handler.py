import json
from urllib.parse import urlparse

import boto3

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

s3_client = boto3.client("s3")

REQUIRED_KEYS = [
    "source_format",
    "rename_columns",
]


class ConfigValidationError(Exception):
    pass


def lambda_handler(event, context):
    config_s3_uri = event["config_s3_uri"]
    config_version = event["config_version"]
    config = read_config(config_s3_uri)
    normalized = normalize_and_validate(config)

    return {
        "config_version": config_version,
        "config_format": normalized["config_format"],
        "defaults_applied": normalized["defaults_applied"],
        "normalized_config": normalized["normalized_config"],
    }


def read_config(config_s3_uri):
    parsed = urlparse(config_s3_uri)
    response = s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    payload = response["Body"].read().decode("utf-8")

    if parsed.path.endswith(".json"):
        return json.loads(payload), "json"
    if parsed.path.endswith((".yaml", ".yml")):
        if yaml is None:
            raise ConfigValidationError("yaml_support_unavailable")
        return yaml.safe_load(payload), "yaml"
    raise ConfigValidationError("unsupported_config_format")


def normalize_and_validate(config_payload):
    config, config_format = config_payload
    if not isinstance(config, dict):
        raise ConfigValidationError("config_must_be_object")

    for key in REQUIRED_KEYS:
        if key not in config:
            raise ConfigValidationError(f"missing_required_key:{key}")

    defaults_applied = []
    normalized = dict(config)

    if "joins" not in normalized:
        normalized["joins"] = []
        defaults_applied.append("joins")
    if "cleaning_rules" not in normalized:
        normalized["cleaning_rules"] = {}
        defaults_applied.append("cleaning_rules")
    if "transformations" not in normalized:
        normalized["transformations"] = {}
        defaults_applied.append("transformations")

    aggregation = normalized.get("gold_aggregations", normalized.get("aggregation"))
    if not isinstance(aggregation, dict):
        raise ConfigValidationError("aggregation_must_be_object")
    if "group_by" not in aggregation:
        raise ConfigValidationError("missing_required_key:aggregation.group_by")
    if "metrics" not in aggregation:
        raise ConfigValidationError("missing_required_key:aggregation.metrics")

    if not isinstance(normalized["rename_columns"], dict) or not all(
        isinstance(source, str) and isinstance(target, str)
        for source, target in normalized["rename_columns"].items()
    ):
        raise ConfigValidationError("invalid_rename_columns")

    if not isinstance(normalized["cleaning_rules"], dict):
        raise ConfigValidationError("invalid_cleaning_rules")

    if not isinstance(normalized["joins"], list):
        raise ConfigValidationError("invalid_joins")
    for join_spec in normalized["joins"]:
        if not isinstance(join_spec, dict):
            raise ConfigValidationError("invalid_join_spec")
        for key in ["path", "keys"]:
            if key not in join_spec:
                raise ConfigValidationError(f"missing_join_key:{key}")
        join_keys = join_spec["keys"]
        if not isinstance(join_keys, (list, str)) or (isinstance(join_keys, list) and len(join_keys) == 0):
            raise ConfigValidationError("invalid_join_keys")
        column_mapping = join_spec.get("column_mapping", {})
        if column_mapping and (
            not isinstance(column_mapping, dict)
            or not all(isinstance(source, str) and isinstance(target, str) for source, target in column_mapping.items())
        ):
            raise ConfigValidationError("invalid_join_column_mapping")

    metrics = aggregation["metrics"]
    if not isinstance(metrics, list) or len(metrics) == 0:
        raise ConfigValidationError("invalid_aggregation_metrics")
    for metric in metrics:
        if not isinstance(metric, dict):
            raise ConfigValidationError("invalid_metric_spec")
        for key in ["column", "function"]:
            if key not in metric:
                raise ConfigValidationError(f"missing_metric_key:{key}")

    if not isinstance(aggregation["group_by"], list):
        raise ConfigValidationError("invalid_aggregation_group_by")
    if not isinstance(normalized["transformations"], dict):
        raise ConfigValidationError("invalid_transformations")
    if "cast_columns" in normalized["transformations"] and not isinstance(
        normalized["transformations"]["cast_columns"], dict
    ):
        raise ConfigValidationError("invalid_cast_columns")
    if "fill_nulls" in normalized["transformations"] and not isinstance(
        normalized["transformations"]["fill_nulls"], dict
    ):
        raise ConfigValidationError("invalid_fill_nulls")
    if "derived_columns" in normalized["transformations"]:
        derived_columns = normalized["transformations"]["derived_columns"]
        if not isinstance(derived_columns, list):
            raise ConfigValidationError("invalid_derived_columns")
        for item in derived_columns:
            if not isinstance(item, dict) or "name" not in item or "expression" not in item:
                raise ConfigValidationError("invalid_derived_column_spec")
    if "filter_expressions" in normalized["transformations"] and not isinstance(
        normalized["transformations"]["filter_expressions"], list
    ):
        raise ConfigValidationError("invalid_filter_expressions")

    return {
        "config_format": config_format,
        "defaults_applied": defaults_applied,
        "normalized_config": normalized,
    }
