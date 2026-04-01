"""Crawl unprocessed feed items and save page content to a Volume.

Entry-point script for the ``spark_python_task`` job.
All heavy lifting lives in the ``databricks_docs`` package;
configuration is read from ``configs/ingestion.yml``.
"""

from __future__ import annotations

from pathlib import Path

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from loguru import logger

from databricks_docs.page_crawler import PageCrawler

CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "ingestion.yml"

crawler = PageCrawler(config_path=CONFIG_PATH)
logger.info("Config: {}", crawler.config.model_dump())

spark = DatabricksSession.builder.getOrCreate()
workspace_client = WorkspaceClient()
count = crawler.run(spark=spark, workspace_client=workspace_client)

logger.info("Done. {} pages crawled and saved to Volume.", count)
