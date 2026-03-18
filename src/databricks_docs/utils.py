"""Shared utility functions used across modules."""

from __future__ import annotations

import datetime
import re
from html.parser import HTMLParser
from time import mktime

import feedparser
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from databricks_docs.config import IngestionConfig

# ---------------------------------------------------------------------------
# Spark schema that mirrors FeedItem
# ---------------------------------------------------------------------------

FEED_SCHEMA = T.StructType(
    [
        T.StructField("guid", T.StringType(), False),
        T.StructField("title", T.StringType(), False),
        T.StructField("link", T.StringType(), False),
        T.StructField("pub_date", T.TimestampType(), False),
        T.StructField("description", T.StringType(), True),
        T.StructField("categories", T.ArrayType(T.StringType()), True),
        T.StructField("source", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("processed", T.BooleanType(), False),
        T.StructField("volume_path", T.StringType(), True),
    ]
)


# ---------------------------------------------------------------------------
# HTML-to-text helper
# ---------------------------------------------------------------------------


class _HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and return plain text."""

    def __init__(self) -> None:
        super().__init__()
        self._pieces: list[str] = []

    def handle_data(self, data: str) -> None:  # noqa: ANN001
        self._pieces.append(data)

    def get_text(self) -> str:
        raw = " ".join(self._pieces)
        return re.sub(r"\s+", " ", raw).strip()


def strip_html(html: str) -> str:
    """Convert an HTML string to plain text."""
    extractor = _HTMLTextExtractor()
    extractor.feed(html)
    return extractor.get_text()


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------


def parse_pub_date(entry: feedparser.FeedParserDict) -> datetime.datetime:
    """Extract a timezone-aware datetime from a feed entry."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime.datetime.fromtimestamp(
            mktime(entry.published_parsed),
            tz=datetime.UTC,
        )
    return datetime.datetime.now(tz=datetime.UTC)


# ---------------------------------------------------------------------------
# Delta table helpers
# ---------------------------------------------------------------------------


def create_table_if_not_exists(
    spark: SparkSession,
    config: IngestionConfig,
) -> None:
    """Create the target schema and Delta table if they don't exist."""
    fqn = config.full_table_name
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.schema_name}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            guid STRING,
            title STRING,
            link STRING,
            pub_date TIMESTAMP,
            description STRING,
            categories ARRAY<STRING>,
            source STRING,
            ingested_at TIMESTAMP,
            processed BOOLEAN,
            volume_path STRING
        )
        USING DELTA
    """)
    logger.info("Ensured table exists: {}", fqn)


def get_watermark(
    spark: SparkSession,
    config: IngestionConfig,
) -> datetime.datetime | None:
    """Read the latest ``pub_date`` from the target table."""
    fqn = config.full_table_name
    row = spark.sql(f"SELECT MAX(pub_date) AS max_pub_date FROM {fqn}").first()

    if row and row["max_pub_date"] is not None:
        wm = row["max_pub_date"].replace(tzinfo=datetime.UTC)
        logger.info("Watermark: {}", wm.isoformat())
        return wm

    logger.info("No watermark found — first run, ingesting all items.")
    return None
