"""Tests for config, utils, and the RSSIngestor class."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from databricks_docs.config import FeedConfig, IngestionConfig
from databricks_docs.rss_ingestor import FeedItem, RSSIngestor
from databricks_docs.utils import FEED_SCHEMA, strip_html

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "ingestion.yml"

# ---------------------------------------------------------------------------
# strip_html tests
# ---------------------------------------------------------------------------


class TestStripHtml:
    """Tests for the HTML-to-text conversion."""

    def test_removes_tags(self) -> None:
        html = "<p>Hello <strong>world</strong></p>"
        assert strip_html(html) == "Hello world"

    def test_handles_nested_tags(self) -> None:
        html = "<div><p>A <a href='#'>link</a> here</p></div>"
        assert strip_html(html) == "A link here"

    def test_collapses_whitespace(self) -> None:
        html = "<p>  lots   of    spaces  </p>"
        assert strip_html(html) == "lots of spaces"

    def test_handles_empty_string(self) -> None:
        assert strip_html("") == ""

    def test_handles_cdata_content(self) -> None:
        html = "Databricks Runtime 18.1 is now GA. See details."
        assert strip_html(html) == "Databricks Runtime 18.1 is now GA. See details."

    def test_handles_list_items(self) -> None:
        html = "<ul><li>Item 1</li><li>Item 2</li></ul>"
        result = strip_html(html)
        assert "Item 1" in result
        assert "Item 2" in result


# ---------------------------------------------------------------------------
# IngestionConfig tests
# ---------------------------------------------------------------------------


class TestIngestionConfig:
    """Tests for the Pydantic config model."""

    def test_defaults(self) -> None:
        cfg = IngestionConfig()
        assert cfg.catalog == "dev"
        assert cfg.schema_name == "llmops"
        assert cfg.table_name == "databricks_feed_items"
        assert cfg.env == "dev"
        assert cfg.full_table_name == "dev.llmops.databricks_feed_items"
        assert len(cfg.feeds) == 1

    def test_custom_values(self) -> None:
        cfg = IngestionConfig(
            catalog="prod",
            schema_name="analytics",
            table_name="feed_data",
            env="prod",
            git_sha="abc123",
            run_id="42",
        )
        assert cfg.full_table_name == "prod.analytics.feed_data"
        assert cfg.git_sha == "abc123"
        assert cfg.run_id == "42"

    def test_custom_feeds(self) -> None:
        cfg = IngestionConfig(
            feeds=[
                FeedConfig(
                    url="https://example.com/feed.xml",
                    source_label="custom",
                ),
            ],
        )
        assert len(cfg.feeds) == 1
        assert cfg.feeds[0].source_label == "custom"

    def test_duplicate_source_labels_rejected(self) -> None:
        with pytest.raises(ValueError, match="Duplicate source_label"):
            IngestionConfig(
                feeds=[
                    FeedConfig(
                        url="https://a.com/feed.xml",
                        source_label="same",
                    ),
                    FeedConfig(
                        url="https://b.com/feed.xml",
                        source_label="same",
                    ),
                ],
            )

    def test_default_feeds_have_correct_urls(self) -> None:
        cfg = IngestionConfig()
        labels = {f.source_label for f in cfg.feeds}
        assert labels == {"blog"}
        urls = {f.url for f in cfg.feeds}
        assert any("databricks.com" in u for u in urls)

    def test_load_from_yaml(self) -> None:
        cfg = IngestionConfig.load(path=CONFIG_PATH)
        assert cfg.catalog != ""
        assert cfg.schema_name != ""
        assert cfg.table_name != ""
        assert len(cfg.feeds) >= 1


# ---------------------------------------------------------------------------
# FeedItem model tests
# ---------------------------------------------------------------------------


class TestFeedItem:
    """Tests for the Pydantic FeedItem model."""

    def test_minimal_construction(self) -> None:
        item = FeedItem(
            guid="test-1",
            title="Test Item",
            link="https://example.com/test",
            pub_date=datetime(2026, 3, 1, tzinfo=UTC),
            description="A test description",
            source="release_notes",
        )
        assert item.guid == "test-1"
        assert item.processed is False
        assert item.volume_path is None
        assert isinstance(item.ingested_at, datetime)

    def test_categories_default_empty(self) -> None:
        item = FeedItem(
            guid="test-2",
            title="Test",
            link="https://example.com",
            pub_date=datetime(2026, 1, 1, tzinfo=UTC),
            description="desc",
            source="blog",
        )
        assert item.categories == []

    def test_all_fields(self) -> None:
        now = datetime.now(tz=UTC)
        item = FeedItem(
            guid="test-3",
            title="Full Item",
            link="https://example.com/full",
            pub_date=now,
            description="Full description",
            categories=["Product", "Generative AI"],
            source="release_notes",
            ingested_at=now,
            processed=True,
            volume_path="/Volumes/dev/llmops/raw/test.txt",
        )
        assert item.processed is True
        assert item.volume_path == "/Volumes/dev/llmops/raw/test.txt"
        assert len(item.categories) == 2


# ---------------------------------------------------------------------------
# RSSIngestor.filter_new_items tests
# ---------------------------------------------------------------------------


class TestFilterNewItems:
    """Tests for the incremental filter logic."""

    def _make_items(self) -> list[FeedItem]:
        return [
            FeedItem(
                guid=f"item-{i}",
                title=f"Item {i}",
                link=f"https://example.com/{i}",
                pub_date=datetime(2026, 1, day, tzinfo=UTC),
                description=f"Desc {i}",
                source="release_notes",
            )
            for i, day in enumerate([5, 10, 15, 20, 25], start=1)
        ]

    def test_no_watermark_returns_all(self) -> None:
        items = self._make_items()
        result = RSSIngestor.filter_new_items(items, None)
        assert len(result) == 5

    def test_filters_by_watermark(self) -> None:
        items = self._make_items()
        watermark = datetime(2026, 1, 15, tzinfo=UTC)
        result = RSSIngestor.filter_new_items(items, watermark)
        assert len(result) == 2
        assert all(item.pub_date > watermark for item in result)

    def test_watermark_after_all_items(self) -> None:
        items = self._make_items()
        watermark = datetime(2026, 2, 1, tzinfo=UTC)
        result = RSSIngestor.filter_new_items(items, watermark)
        assert len(result) == 0

    def test_naive_watermark_treated_as_utc(self) -> None:
        items = self._make_items()
        watermark = datetime(2026, 1, 15)  # noqa: DTZ001
        result = RSSIngestor.filter_new_items(items, watermark)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# RSSIngestor.fetch_feed live tests
# ---------------------------------------------------------------------------


class TestFetchFeedLive:
    """Integration tests that hit the actual RSS feeds."""

    def test_blog_feed(self) -> None:
        ingestor = RSSIngestor(config_path=CONFIG_PATH)
        blog_feed = next(f for f in ingestor.config.feeds if f.source_label == "blog")
        items = ingestor.fetch_feed(blog_feed.url, blog_feed.source_label)
        assert len(items) > 0
        first = items[0]
        assert first.source == "blog"
        assert first.link.startswith("https://")
        # guid must differ from link (hash-based)
        assert first.guid != first.link
        assert len(first.guid) == 64  # SHA-256 hex digest


# ---------------------------------------------------------------------------
# Delta utils tests
# ---------------------------------------------------------------------------


class TestDeltaUtils:
    """Tests for the Delta table utilities."""

    def test_feed_schema_field_names(self) -> None:
        names = [f.name for f in FEED_SCHEMA.fields]
        assert names == [
            "guid",
            "title",
            "link",
            "pub_date",
            "description",
            "categories",
            "source",
            "ingested_at",
            "processed",
            "volume_path",
        ]

    def test_rss_ingestor_run_is_callable(self) -> None:
        """Smoke test: RSSIngestor.run is callable."""
        ingestor = RSSIngestor(config_path=CONFIG_PATH)
        assert callable(ingestor.run)
