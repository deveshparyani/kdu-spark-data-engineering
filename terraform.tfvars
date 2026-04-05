environment            = "dev"
aws_region             = "ap-southeast-1"
adscribe_api_url       = "https://api.adscribe.example.com/v1/exports/daily"
adscribe_secret_string = "{\"api_key\":\"replace-me\",\"client\":\"replace-me\"}"
alert_email            = "data-engineering@example.com"

redshift_admin_username      = "kduadmin"
redshift_base_capacity       = 32
redshift_publicly_accessible = true
default_config_version       = "v1"
redshift_staging_table       = "public.kdu_spark_staging_fact_metrics"

silver_number_of_workers   = 2
gold_number_of_workers     = 2
silver_max_concurrent_runs = 1
gold_max_concurrent_runs   = 1
ingestion_queue_batch_size = 10

additional_tags = {
  Owner = "data-platform"
}
