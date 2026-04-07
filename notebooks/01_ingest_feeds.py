"""Ingest Databricks blog RSS feed into a Delta table."""

from __future__ import annotations

import argparse
import os

from loguru import logger
from pyspark.sql import Row

from databricks_docs.rss_ingestor import RSSIngestor

# ------------------------------------------------------------------
# Spark Connection setup
# ------------------------------------------------------------------

if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    logger.info("Running on Databricks. Setting up SparkSession.")
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
else:
    logger.info("Running on local machine. Setting up Databricks connect.")
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless().getOrCreate()

# ------------------------------------------------------------------
# Argument parsing
# ------------------------------------------------------------------

parser = argparse.ArgumentParser()

parser.add_argument(
    "--root_path",
    type=str,
    help="Root path of the project in Databricks Workspace",
)
parser.add_argument(
    "--config_path",
    type=str,
    help="Path to yaml with ingestion configuration, relative to root_path",
)
parser.add_argument(
    "--env",
    type=str,
    default="test",
    help="Environment to use for Databricks configuration",
)
parser.add_argument(
    "--git_sha",
    type=str,
    default="test",
    help="Git SHA to use for code reference",
)
parser.add_argument(
    "--run_id",
    type=str,
    default="local",
    help="Run ID to use for job reference",
)

args, unknown = parser.parse_known_args()
if unknown:
    logger.info(f"Ignored unknown arguments: {unknown}")

# ------------------------------------------------------------------
# Feed ingestion
# ------------------------------------------------------------------

ingestor = RSSIngestor(config_path=os.path.join(args.root_path, args.config_path))
logger.info("Config: {}", ingestor.config.model_dump())

spark = DatabricksSession.builder.serverless().getOrCreate()
count = ingestor.run(spark)

total: Row = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {ingestor.config.full_table_name}"
).first()
logger.info("Done. {} new, {} total.", count, total["cnt"])
