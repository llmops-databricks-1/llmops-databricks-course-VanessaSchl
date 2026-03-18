"""Pydantic configuration models for the ingestion pipeline."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, model_validator

# Default path to the config file, relative to the project root.
_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "ingestion.yml"


class FeedConfig(BaseModel):
    """Configuration for a single RSS feed source."""

    url: str = Field(..., description="URL of the RSS feed")
    source_label: str = Field(..., description="Label identifying the source")


class IngestionConfig(BaseModel):
    """Configuration for the RSS feed ingestion pipeline.

    Load from the YAML file in ``configs/ingestion.yml`` via
    ``IngestionConfig.load()``.  All parameters that previously
    required CLI arguments are now read from that file.
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
    feeds: list[FeedConfig] = Field(
        default_factory=lambda: [
            FeedConfig(
                url="https://learn.microsoft.com/en-us/azure/databricks/feed.xml",
                source_label="release_notes",
            ),
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
    def load(cls, path: str | Path | None = None) -> IngestionConfig:
        """Load configuration from a YAML file.

        Parameters
        ----------
        path
            Path to the YAML config file.  Defaults to
            ``<project_root>/configs/ingestion.yml``.
        """
        config_path = Path(path) if path else _DEFAULT_CONFIG_PATH
        with open(config_path) as fh:
            data = yaml.safe_load(fh)
        return cls(**data)
