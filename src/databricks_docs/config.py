"""Pydantic configuration models for the ingestion pipeline."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, model_validator


class FeedConfig(BaseModel):
    """Configuration for a single RSS feed source."""

    url: str = Field(..., description="URL of the RSS feed")
    source_label: str = Field(..., description="Label identifying the source")


class IngestionConfig(BaseModel):
    """Configuration for the RSS feed ingestion pipeline.

    Load from a YAML file via ``IngestionConfig.load()``.
    """

    catalog: str = Field(
        default="dev",
        description="Unity Catalog name",
    )
    schema_name: str = Field(
        default="llmops",
        description="Schema name within the catalog",
    )
    table_name: str = Field(
        default="databricks_feed_items",
        description="Delta table name for feed items",
    )
    env: str = Field(
        default="dev",
        description="Deployment environment (dev / acc / prd)",
    )
    git_sha: str = Field(
        default="",
        description="Git commit SHA for lineage",
    )
    run_id: str = Field(
        default="",
        description="Databricks job run ID",
    )
    volume_name: str = Field(
        default="raw_pages",
        description="Unity Catalog Volume name for crawled pages",
    )
    crawl_delay_seconds: float = Field(
        default=1.5,
        description="Delay in seconds between HTTP requests when crawling",
    )
    feeds: list[FeedConfig] = Field(
        default_factory=lambda: [
            FeedConfig(
                url="https://www.databricks.com/rss.xml",
                source_label="blog",
            ),
        ],
        description="List of RSS feeds to ingest",
    )

    # ------------------------------------------------------------------
    # Computed helpers
    # ------------------------------------------------------------------

    @property
    def full_table_name(self) -> str:
        """Return the fully qualified Delta table name."""
        return f"{self.catalog}.{self.schema_name}.{self.table_name}"

    @property
    def volume_base_path(self) -> str:
        """Return the Volumes path for crawled pages."""
        return f"/Volumes/{self.catalog}/{self.schema_name}/{self.volume_name}"

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def _validate_feeds_unique(self) -> IngestionConfig:
        """Ensure no duplicate source labels across feeds."""
        labels = [f.source_label for f in self.feeds]
        if len(labels) != len(set(labels)):
            msg = f"Duplicate source_label in feeds: {labels}"
            raise ValueError(msg)
        return self

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def load(cls, path: str | Path) -> IngestionConfig:
        """Load configuration from a YAML file.

        Parameters
        ----------
        path
            Path to the YAML config file.
        """
        config_path = Path(path)
        with open(config_path) as fh:
            data = yaml.safe_load(fh)
        return cls(**data)
