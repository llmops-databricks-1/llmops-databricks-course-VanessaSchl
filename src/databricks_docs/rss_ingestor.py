"""RSS feed ingestion for Databricks blog posts."""

from __future__ import annotations

import datetime
import hashlib
from pathlib import Path

import feedparser
from loguru import logger
from pydantic import BaseModel, Field
from pyspark.sql import Row, SparkSession

from databricks_docs.config import IngestionConfig
from databricks_docs.utils import (
    FEED_SCHEMA,
    create_table_if_not_exists,
    get_watermark,
    parse_pub_date,
    strip_html,
)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


class FeedItem(BaseModel):
    """A single item from an RSS feed."""

    guid: str
    title: str
    link: str
    pub_date: datetime.datetime
    description: str
    categories: list[str] = Field(default_factory=list)
    source: str
    ingested_at: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(tz=datetime.UTC),
    )
    processed: bool = False
    volume_path: str | None = None


# ---------------------------------------------------------------------------
# RSSIngestor
# ---------------------------------------------------------------------------


class RSSIngestor:
    """End-to-end RSS ingestion pipeline.

    Parameters
    ----------
    config_path
        Path to the YAML configuration file.  Defaults to
        ``configs/ingestion.yml`` at the project root.
    """

    def __init__(self, config_path: str | Path | None = None) -> None:
        self.config = IngestionConfig.load(config_path)

    # ------------------------------------------------------------------
    # Feed fetching
    # ------------------------------------------------------------------

    def fetch_feed(
        self,
        url: str,
        source_label: str,
    ) -> list[FeedItem]:
        """Fetch and parse a single RSS feed into ``FeedItem`` objects."""
        logger.info("Fetching feed: {} (source={})", url, source_label)
        feed = feedparser.parse(url)

        if feed.bozo and not feed.entries:
            raise RuntimeError(f"Failed to parse feed at {url}: {feed.bozo_exception}")

        items: list[FeedItem] = []
        for entry in feed.entries:
            link = entry.get("link", "")
            guid = hashlib.sha256(
                f"{source_label}:{link}".encode(),
            ).hexdigest()
            item = FeedItem(
                guid=guid,
                title=entry.get("title", "").strip(),
                link=link,
                pub_date=parse_pub_date(entry),
                description=strip_html(
                    entry.get("summary", entry.get("description", "")),
                ),
                categories=[
                    tag.get("term", tag.get("label", "")) for tag in entry.get("tags", [])
                ],
                source=source_label,
            )
            items.append(item)

        logger.info(
            "Parsed {} items from {} (source={})",
            len(items),
            url,
            source_label,
        )
        return items

    def fetch_all_feeds(self) -> list[FeedItem]:
        """Fetch items from every feed defined in the config."""
        all_items: list[FeedItem] = []
        for feed_cfg in self.config.feeds:
            items = self.fetch_feed(feed_cfg.url, feed_cfg.source_label)
            all_items.extend(items)
        logger.info(
            "Total items fetched across all feeds: {}",
            len(all_items),
        )
        return all_items

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    @staticmethod
    def filter_new_items(
        items: list[FeedItem],
        last_ingested_date: datetime.datetime | None,
    ) -> list[FeedItem]:
        """Return only items published after *last_ingested_date*.

        If *last_ingested_date* is ``None`` (first run), all items are
        returned.
        """
        if last_ingested_date is None:
            logger.info(
                "No watermark found — returning all {} items",
                len(items),
            )
            return items

        if last_ingested_date.tzinfo is None:
            last_ingested_date = last_ingested_date.replace(
                tzinfo=datetime.UTC,
            )

        new_items = [item for item in items if item.pub_date > last_ingested_date]
        logger.info(
            "Filtered to {} new items (watermark={})",
            len(new_items),
            last_ingested_date.isoformat(),
        )
        return new_items

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def run(self, spark: SparkSession) -> int:
        """Execute the full ingestion pipeline.

        Creates the Delta table if needed, reads the watermark,
        fetches all configured feeds, filters to new items, and
        appends them.

        Returns the number of newly ingested items.
        """
        create_table_if_not_exists(spark, self.config)
        watermark = get_watermark(spark, self.config)

        all_items = self.fetch_all_feeds()
        new_items = self.filter_new_items(all_items, watermark)

        if not new_items:
            logger.info("No new items to ingest.")
            return 0

        rows = [
            Row(
                guid=item.guid,
                title=item.title,
                link=item.link,
                pub_date=item.pub_date,
                description=item.description,
                categories=item.categories,
                source=item.source,
                ingested_at=item.ingested_at,
                processed=item.processed,
                volume_path=item.volume_path,
            )
            for item in new_items
        ]

        df = spark.createDataFrame(rows, schema=FEED_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(
            self.config.full_table_name,
        )

        date_min = min(i.pub_date for i in new_items).date()
        date_max = max(i.pub_date for i in new_items).date()
        logger.info(
            "Ingested {} new items into {}. Date range: {} — {}",
            len(new_items),
            self.config.full_table_name,
            date_min,
            date_max,
        )
        return len(new_items)
