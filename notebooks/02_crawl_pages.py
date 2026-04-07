"""Crawl unprocessed feed items and save page content to a Volume."""

from __future__ import annotations

import argparse
import os

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from loguru import logger

from databricks_docs.page_crawler import PageCrawler

# ------------------------------------------------------------------
# Spark Connection setup
# ------------------------------------------------------------------

local = "DATABRICKS_RUNTIME_VERSION" not in os.environ
if not local:
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
# Feed Crawling
# ------------------------------------------------------------------

crawler = PageCrawler(config_path=os.path.join(args.root_path, args.config_path))
logger.info("Config: {}", crawler.config.model_dump())

workspace_client = WorkspaceClient() if local else None
count = crawler.run(local=local, spark=spark, workspace_client=workspace_client)

logger.info("Done. {} pages crawled and saved to Volume.", count)
