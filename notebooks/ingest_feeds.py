"""Ingest Databricks blog RSS feed into a Delta table.

Entry-point script for the ``spark_python_task`` job.
All heavy lifting lives in the ``databricks_docs`` package;
configuration is read from ``configs/ingestion.yml``.
"""

from __future__ import annotations

from pathlib import Path

from databricks.connect import DatabricksSession
from loguru import logger
from pyspark.sql import Row

from databricks_docs.rss_ingestor import RSSIngestor

CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "ingestion.yml"

ingestor = RSSIngestor(config_path=CONFIG_PATH)
logger.info("Config: {}", ingestor.config.model_dump())

spark = DatabricksSession.builder.getOrCreate()
count = ingestor.run(spark)

total: Row = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {ingestor.config.full_table_name}"
).first()
logger.info("Done. {} new, {} total.", count, total["cnt"])
