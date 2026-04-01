"""Tests for the PageCrawler class."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from databricks_docs.config import IngestionConfig
from databricks_docs.page_crawler import PageCrawler

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "ingestion.yml"


class TestConfigVolumeFields:
    """Verify new config fields for crawling."""

    def test_defaults(self) -> None:
        cfg = IngestionConfig()
        assert cfg.volume_name == "raw_pages"
        assert cfg.crawl_delay_seconds == 1.5

    def test_volume_base_path(self) -> None:
        cfg = IngestionConfig(
            catalog="my_cat",
            schema_name="my_schema",
            volume_name="pages",
        )
        assert cfg.volume_base_path == "/Volumes/my_cat/my_schema/pages"

    def test_custom_crawl_delay(self) -> None:
        cfg = IngestionConfig(crawl_delay_seconds=0.5)
        assert cfg.crawl_delay_seconds == 0.5


# ---------------------------------------------------------------------------
# save_to_volume tests
# ---------------------------------------------------------------------------


class TestSaveToVolume:
    """Tests for writing extracted text to file."""

    def test_creates_directory_and_file(self) -> None:
        mock_workspace_client = MagicMock()
        path = PageCrawler.save_to_volume(
            workspace_client=mock_workspace_client,
            volume_base_path="/Volumes/my_cat/my_schema/pages",
            title="release_notes",
            source="item-1",
            text="Hello world",
        )
        assert path == "/Volumes/my_cat/my_schema/pages/item-1/release_notes.txt"

    def test_sanitises_title(self) -> None:
        mock_workspace_client = MagicMock()
        path = PageCrawler.save_to_volume(
            workspace_client=mock_workspace_client,
            volume_base_path="/Volumes/my_cat/my_schema/pages",
            title="My Blog Post: Version 1.0?",
            source="blog",
            text="content",
        )
        filename = path.split("/")[-1]
        assert ":" not in filename
        assert "?" not in filename
        assert filename.endswith(".txt")


# ---------------------------------------------------------------------------
# crawl_page tests
# ---------------------------------------------------------------------------


class TestCrawlPage:
    """Tests for the crawl_page static method (mocked network)."""

    @patch("databricks_docs.page_crawler.trafilatura")
    def test_returns_extracted_text(
        self,
        mock_traf: MagicMock,
    ) -> None:
        mock_traf.fetch_url.return_value = "<html><body>Hello</body></html>"
        mock_traf.extract.return_value = "Hello"

        result = PageCrawler.crawl_page("https://example.com")
        assert result == "Hello"
        mock_traf.fetch_url.assert_called_once_with("https://example.com")

    @patch("databricks_docs.page_crawler.trafilatura")
    def test_returns_none_on_fetch_failure(
        self,
        mock_traf: MagicMock,
    ) -> None:
        mock_traf.fetch_url.return_value = None

        result = PageCrawler.crawl_page("https://example.com/404")
        assert result is None

    @patch("databricks_docs.page_crawler.trafilatura")
    def test_returns_none_when_no_text_extracted(
        self,
        mock_traf: MagicMock,
    ) -> None:
        mock_traf.fetch_url.return_value = "<html></html>"
        mock_traf.extract.return_value = None

        result = PageCrawler.crawl_page("https://example.com/empty")
        assert result is None


# ---------------------------------------------------------------------------
# get_unprocessed tests
# ---------------------------------------------------------------------------


class TestGetUnprocessed:
    """Tests for the SQL query helper (mocked Spark)."""

    def test_returns_collected_rows(self) -> None:
        mock_spark = MagicMock()
        mock_rows = [
            {"guid": "g1", "link": "https://a.com", "source": "blog"},
            {"guid": "g2", "link": "https://b.com", "source": "release_notes"},
        ]
        mock_spark.sql.return_value.collect.return_value = mock_rows

        cfg = IngestionConfig()
        rows = PageCrawler.get_unprocessed(mock_spark, cfg)
        assert len(rows) == 2
        mock_spark.sql.assert_called_once()


# ---------------------------------------------------------------------------
# mark_processed tests
# ---------------------------------------------------------------------------


class TestMarkProcessed:
    """Tests for the UPDATE helper (mocked Spark)."""

    def test_executes_update_sql(self) -> None:
        mock_spark = MagicMock()
        cfg = IngestionConfig()

        PageCrawler.mark_processed(
            mock_spark,
            cfg,
            "guid-123",
            "/Volumes/dev/llmops/raw_pages/blog/guid-123.txt",
        )

        call_args = mock_spark.sql.call_args[0][0]
        assert "UPDATE" in call_args
        assert "guid-123" in call_args
        assert "processed = true" in call_args


# ---------------------------------------------------------------------------
# run orchestration tests
# ---------------------------------------------------------------------------


class TestRun:
    """Integration tests for the run method (mocked Spark + network)."""

    @patch("databricks_docs.page_crawler.time.sleep")
    @patch.object(PageCrawler, "crawl_page")
    @patch.object(PageCrawler, "mark_processed")
    @patch.object(PageCrawler, "save_to_volume")
    @patch.object(PageCrawler, "get_unprocessed")
    def test_crawls_and_saves(
        self,
        mock_get: MagicMock,
        mock_save: MagicMock,
        mock_mark: MagicMock,
        mock_crawl: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        mock_spark = MagicMock()
        mock_workspace_client = MagicMock()
        mock_get.return_value = [
            {"guid": "g1", "link": "https://a.com", "title": "Title 1", "source": "blog"},
        ]
        mock_crawl.return_value = "Page text"
        mock_save.return_value = "/Volumes/dev/llmops/raw_pages/blog/g1.txt"

        crawler = PageCrawler(config_path=CONFIG_PATH)
        crawler.config.crawl_delay_seconds = 0  # fast tests
        count = crawler.run(spark=mock_spark, workspace_client=mock_workspace_client)

        assert count == 1
        mock_crawl.assert_called_once_with("https://a.com")
        mock_save.assert_called_once()
        mock_mark.assert_called_once()

    @patch.object(PageCrawler, "get_unprocessed")
    def test_returns_zero_when_nothing_to_crawl(
        self,
        mock_get: MagicMock,
    ) -> None:
        mock_spark = MagicMock()
        mock_workspace_client = MagicMock()
        mock_get.return_value = []

        crawler = PageCrawler(config_path=CONFIG_PATH)
        count = crawler.run(spark=mock_spark, workspace_client=mock_workspace_client)
        assert count == 0

    @patch("databricks_docs.page_crawler.time.sleep")
    @patch.object(PageCrawler, "crawl_page")
    @patch.object(PageCrawler, "get_unprocessed")
    def test_continues_on_crawl_failure(
        self,
        mock_get: MagicMock,
        mock_crawl: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        mock_spark = MagicMock()
        mock_workspace_client = MagicMock()
        mock_get.return_value = [
            {
                "guid": "g1",
                "link": "https://fail.com",
                "title": "Title 1",
                "source": "blog",
            },
            {
                "guid": "g2",
                "link": "https://ok.com",
                "title": "Title 2",
                "source": "blog",
            },
        ]
        # First call raises, second returns None (simulates no text).
        mock_crawl.side_effect = [RuntimeError("boom"), None]

        crawler = PageCrawler(config_path=CONFIG_PATH)
        crawler.config.crawl_delay_seconds = 0
        count = crawler.run(spark=mock_spark, workspace_client=mock_workspace_client)

        assert count == 0
        assert mock_crawl.call_count == 2
