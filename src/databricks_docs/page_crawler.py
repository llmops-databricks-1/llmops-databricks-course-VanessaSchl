"""Crawl unprocessed feed-item links and save extracted text to a Volume."""

from __future__ import annotations

import io
import time
from pathlib import Path

import backoff
import trafilatura
from databricks.sdk import WorkspaceClient
from loguru import logger
from pyspark.sql import Row, SparkSession

from databricks_docs.config import IngestionConfig

# Maximum retries for transient HTTP errors.
_MAX_TRIES = 3


class PageCrawler:
    """Crawl pages referenced by unprocessed feed items.

    Reads rows where ``processed = False`` from the Delta table,
    downloads each page, extracts the main text content using
    *trafilatura*, writes it to a Unity Catalog Volume, and marks
    the row as processed.

    Parameters
    ----------
    config_path
        Path to the YAML configuration file.
    """

    def __init__(self, config_path: str | Path) -> None:
        self.config = IngestionConfig.load(config_path)

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    @staticmethod
    def get_unprocessed(
        spark: SparkSession,
        config: IngestionConfig,
    ) -> list[Row]:
        """Return all rows with ``processed = False``."""
        rows = spark.sql(
            f"SELECT guid, link, title, source "
            f"FROM {config.full_table_name} "
            f"WHERE processed = false",
        ).collect()
        logger.info("Found {} unprocessed items", len(rows))
        return rows

    # ------------------------------------------------------------------
    # Crawling
    # ------------------------------------------------------------------

    @staticmethod
    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=_MAX_TRIES,
        on_backoff=lambda details: logger.warning(
            "Retry {tries}/{max_tries} for crawl after {wait:.1f}s",
            **details,
        ),
    )
    def crawl_page(url: str) -> str | None:
        """Download *url* and extract the main text content.

        Returns ``None`` when the page cannot be fetched or has no
        extractable text.
        """
        downloaded = trafilatura.fetch_url(url)
        if downloaded is None:
            logger.warning("Could not download: {}", url)
            return None

        text = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=True,
            include_images=True,
            output_format="txt",
        )
        if not text:
            logger.warning("No extractable text at: {}", url)
            return None

        return text

    # ------------------------------------------------------------------
    # Volume I/O
    # ------------------------------------------------------------------

    @staticmethod
    def save_to_volume(
        workspace_client: WorkspaceClient,
        volume_base_path: str,
        title: str,
        source: str,
        text: str,
    ) -> str:
        """Write *text* to a file inside the Volume and return the path.

        Directory structure::

            {volume_base_path}/{source}/{file_name}.txt
        """

        file_path = f"{volume_base_path}/{source}"
        title_clean = (
            title.lower()
            .replace("/", "_")
            .replace("&", "")
            .replace("?", "")
            .replace(":", "")
            .replace(" ", "_")
        )
        file_name = f"{title_clean}.txt"

        workspace_client.files.create_directory(file_path)
        workspace_client.files.upload(
            file_path=f"{file_path}/{file_name}",
            contents=io.BytesIO(text.encode("utf-8")),
            overwrite=True,
        )
        logger.debug("Saved {} bytes to {}", len(text), file_path)
        return f"{file_path}/{file_name}"

    # ------------------------------------------------------------------
    # Table update
    # ------------------------------------------------------------------

    @staticmethod
    def mark_processed(
        spark: SparkSession,
        config: IngestionConfig,
        guid: str,
        volume_path: str,
    ) -> None:
        """Set ``processed = True`` and ``volume_path`` for a single row."""
        # Use parameterised marker to avoid SQL injection from guid values.
        spark.sql(
            f"UPDATE {config.full_table_name} "
            f"SET processed = true, volume_path = '{volume_path}' "
            f"WHERE guid = '{guid}'",
        )

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def run(self, spark: SparkSession, workspace_client: WorkspaceClient) -> int:
        """Execute the full crawl pipeline.

        1. Query unprocessed rows.
        2. For each row, crawl the link.
        3. Save the extracted text to the Volume.
        4. Update the Delta row.

        Returns the number of successfully crawled pages.
        """
        rows = self.get_unprocessed(spark, self.config)
        if not rows:
            logger.info("No unprocessed items to crawl.")
            return 0

        # Ensure the Volume exists.
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS "
            f"{self.config.catalog}.{self.config.schema_name}"
            f".{self.config.volume_name}",
        )

        success_count = 0
        for row in rows:
            guid: str = row["guid"]
            title: str = row["title"]
            link: str = row["link"]
            source: str = row["source"]

            logger.info("Crawling [{}/{}]: {}", source, title, link)
            try:
                text = self.crawl_page(link)
            except Exception:
                logger.exception("Failed to crawl {}", link)
                continue

            if text is None:
                continue

            volume_path = self.save_to_volume(
                workspace_client=workspace_client,
                volume_base_path=self.config.volume_base_path,
                title=title,
                source=source,
                text=text,
            )
            self.mark_processed(spark, self.config, guid, volume_path)
            success_count += 1

            # Polite crawl delay.
            time.sleep(self.config.crawl_delay_seconds)

        logger.info(
            "Crawled {}/{} pages successfully",
            success_count,
            len(rows),
        )
        return success_count
