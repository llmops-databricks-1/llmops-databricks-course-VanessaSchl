"""Tests for the VectorSearchManager class."""

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

from databricks.sdk import WorkspaceClient

from databricks_docs.config import IngestionConfig
from databricks_docs.vector_search_manager import _SYNC_COLUMNS, VectorSearchManager

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).resolve().parent.parent / "configs" / "ingestion.yml"

VSC_PATCH = "databricks_docs.vector_search_manager.VectorSearchClient"


def _make_manager(mock_vsc: MagicMock | None = None) -> VectorSearchManager:
    """Create a VectorSearchManager with a mocked VectorSearchClient (local=False)."""
    with patch(VSC_PATCH) as vsc_cls:
        if mock_vsc is not None:
            vsc_cls.return_value = mock_vsc
        mgr = VectorSearchManager(
            local=False,
            config_path=CONFIG_PATH,
            workspace_client=None,
        )
    if mock_vsc is not None:
        mgr.vs_client = mock_vsc
    return mgr


def _make_local_manager(
    mock_vsc: MagicMock | None = None,
) -> tuple[VectorSearchManager, MagicMock]:
    """Create a VectorSearchManager with mocked clients (local=True).

    Uses ``spec=WorkspaceClient`` so that ``isinstance`` checks pass.
    """
    mock_wc = MagicMock(spec=WorkspaceClient)
    with patch(VSC_PATCH) as vsc_cls:
        if mock_vsc is not None:
            vsc_cls.return_value = mock_vsc
        mgr = VectorSearchManager(
            local=True,
            config_path=CONFIG_PATH,
            workspace_client=mock_wc,
        )
    if mock_vsc is not None:
        mgr.vs_client = mock_vsc
    return mgr, mock_wc


# ---------------------------------------------------------------------------
# Config field tests
# ---------------------------------------------------------------------------


class TestConfigEmbeddingFields:
    """Verify config fields for embedding."""

    def test_defaults(self) -> None:
        cfg = IngestionConfig()
        assert cfg.chunks_table_name == "page_chunks"
        assert cfg.embedding_model == "databricks-gte-large-en"
        assert cfg.chunk_size == 1000
        assert cfg.chunk_overlap == 200
        assert cfg.vector_search_endpoint_name == "llmops_vs_endpoint"
        assert cfg.vector_search_index_suffix == "page_chunks_index"

    def test_full_chunks_table_name(self) -> None:
        cfg = IngestionConfig(
            catalog="my_cat",
            schema_name="my_schema",
            chunks_table_name="my_chunks",
        )
        assert cfg.full_chunks_table_name == "my_cat.my_schema.my_chunks"

    def test_full_index_name(self) -> None:
        cfg = IngestionConfig(
            catalog="my_cat",
            schema_name="my_schema",
            vector_search_index_suffix="my_idx",
        )
        assert cfg.full_index_name == "my_cat.my_schema.my_idx"

    def test_custom_chunk_params(self) -> None:
        cfg = IngestionConfig(chunk_size=500, chunk_overlap=50)
        assert cfg.chunk_size == 500
        assert cfg.chunk_overlap == 50


# ---------------------------------------------------------------------------
# create_chunks_table tests
# ---------------------------------------------------------------------------


class TestCreateChunksTable:
    """Verify DDL execution for the chunks table."""

    def test_creates_schema_and_table(self) -> None:
        mock_spark = MagicMock()
        mgr = _make_manager()

        mgr.create_chunks_table(mock_spark)

        calls = [c[0][0] for c in mock_spark.sql.call_args_list]
        assert any("CREATE SCHEMA" in c for c in calls)
        assert any("CREATE TABLE" in c for c in calls)
        assert any(mgr.config.full_chunks_table_name in c for c in calls)

    def test_table_has_required_columns(self) -> None:
        mock_spark = MagicMock()
        mgr = _make_manager()

        mgr.create_chunks_table(mock_spark)

        ddl_calls = [
            c[0][0] for c in mock_spark.sql.call_args_list if "CREATE TABLE" in c[0][0]
        ]
        assert len(ddl_calls) == 1
        ddl = ddl_calls[0]
        for col in [
            "chunk_id",
            "chunk_text",
            "source_guid",
            "source_label",
            "title",
            "chunk_index",
            "chunked_at",
        ]:
            assert col in ddl

    def test_enables_change_data_feed(self) -> None:
        mock_spark = MagicMock()
        mgr = _make_manager()

        mgr.create_chunks_table(mock_spark)

        ddl_calls = [
            c[0][0] for c in mock_spark.sql.call_args_list if "CREATE TABLE" in c[0][0]
        ]
        assert "delta.enableChangeDataFeed" in ddl_calls[0]


# ---------------------------------------------------------------------------
# get_unprocessed_files tests
# ---------------------------------------------------------------------------


class TestGetUnprocessedFiles:
    """Verify the LEFT ANTI JOIN query (mocked Spark)."""

    def test_returns_collected_rows(self) -> None:
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {
                "guid": "g1",
                "volume_path": "/Volumes/cat/s/v/blog/post.txt",
                "source": "blog",
                "title": "Post",
            },
        ]

        mgr = _make_manager()
        rows = mgr.get_unprocessed_files(mock_spark)
        assert len(rows) == 1

        sql = mock_spark.sql.call_args[0][0]
        assert "LEFT ANTI JOIN" in sql
        assert mgr.config.full_chunks_table_name in sql


# ---------------------------------------------------------------------------
# read_and_chunk tests
# ---------------------------------------------------------------------------


class TestReadAndChunk:
    """Tests for file reading and recursive character splitting."""

    def test_splits_long_text_local(self) -> None:
        mgr, mock_wc = _make_local_manager()
        text = "Hello world. " * 200
        mock_wc.files.download.return_value.contents.read.return_value = text.encode(
            "utf-8"
        )
        mgr.config = IngestionConfig(chunk_size=500, chunk_overlap=100)

        chunks = mgr.read_and_chunk("/Volumes/cat/s/v/blog/post.txt")

        assert len(chunks) > 1
        for chunk in chunks:
            assert len(chunk) <= 500
        mock_wc.files.download.assert_called_once_with("/Volumes/cat/s/v/blog/post.txt")

    def test_returns_empty_list_for_blank_file_local(self) -> None:
        mgr, mock_wc = _make_local_manager()
        mock_wc.files.download.return_value.contents.read.return_value = b"   \n  "

        chunks = mgr.read_and_chunk("/Volumes/cat/s/v/blog/empty.txt")
        assert chunks == []

    def test_short_text_single_chunk_local(self) -> None:
        mgr, mock_wc = _make_local_manager()
        mock_wc.files.download.return_value.contents.read.return_value = b"Short text."

        chunks = mgr.read_and_chunk("/Volumes/cat/s/v/blog/short.txt")
        assert len(chunks) == 1
        assert chunks[0] == "Short text."

    def test_reads_from_filesystem_when_not_local(self, tmp_path: Path) -> None:
        mgr = _make_manager()
        file = tmp_path / "page.txt"
        file.write_text("Some content from the filesystem.")

        chunks = mgr.read_and_chunk(str(file))
        assert len(chunks) == 1
        assert chunks[0] == "Some content from the filesystem."


# ---------------------------------------------------------------------------
# Endpoint management tests
# ---------------------------------------------------------------------------


class TestEndpointManagement:
    """Tests for create_endpoint_if_not_exists."""

    def test_creates_endpoint_when_missing(self) -> None:
        mock_vsc = MagicMock()
        mock_vsc.list_endpoints.return_value = {"endpoints": []}

        mgr = _make_manager(mock_vsc)
        mgr.create_endpoint_if_not_exists()

        mock_vsc.create_endpoint_and_wait.assert_called_once_with(
            name=mgr.config.vector_search_endpoint_name,
            endpoint_type="STANDARD",
        )

    def test_skips_creation_when_endpoint_exists(self) -> None:
        mock_vsc = MagicMock()
        mgr = _make_manager(mock_vsc)
        mock_vsc.list_endpoints.return_value = {
            "endpoints": [{"name": mgr.config.vector_search_endpoint_name}]
        }

        mgr.create_endpoint_if_not_exists()

        mock_vsc.create_endpoint_and_wait.assert_not_called()


# ---------------------------------------------------------------------------
# Index management tests
# ---------------------------------------------------------------------------


class TestCreateOrGetIndex:
    """Tests for create_or_get_index."""

    def test_returns_existing_index(self) -> None:
        mock_vsc = MagicMock()
        mock_index = MagicMock()
        mock_vsc.get_index.return_value = mock_index
        mock_vsc.list_endpoints.return_value = {"endpoints": []}

        mgr = _make_manager(mock_vsc)
        result = mgr.create_or_get_index()

        assert result is mock_index
        mock_vsc.create_delta_sync_index.assert_not_called()

    def test_creates_index_when_not_found(self) -> None:
        mock_vsc = MagicMock()
        mock_vsc.get_index.side_effect = Exception("not found")
        mock_vsc.list_endpoints.return_value = {"endpoints": []}
        mock_new_index = MagicMock()
        mock_vsc.create_delta_sync_index.return_value = mock_new_index

        mgr = _make_manager(mock_vsc)
        result = mgr.create_or_get_index()

        assert result is mock_new_index
        kw = mock_vsc.create_delta_sync_index.call_args[1]
        assert kw["pipeline_type"] == "TRIGGERED"
        assert kw["primary_key"] == "chunk_id"
        assert kw["embedding_source_column"] == "chunk_text"
        assert kw["embedding_model_endpoint_name"] == mgr.config.embedding_model

    def test_handles_race_condition_already_exists(self) -> None:
        mock_vsc = MagicMock()
        mock_vsc.list_endpoints.return_value = {"endpoints": []}
        mock_index = MagicMock()
        # get_index fails first, succeeds on retry after RESOURCE_ALREADY_EXISTS
        mock_vsc.get_index.side_effect = [Exception("not found"), mock_index]
        mock_vsc.create_delta_sync_index.side_effect = Exception(
            "RESOURCE_ALREADY_EXISTS"
        )

        mgr = _make_manager(mock_vsc)
        result = mgr.create_or_get_index()

        assert result is mock_index


class TestSyncIndex:
    """Tests for sync_index."""

    def test_syncs_existing_index(self) -> None:
        mock_vsc = MagicMock()
        mock_index = MagicMock()
        mock_vsc.get_index.return_value = mock_index
        mock_vsc.list_endpoints.return_value = {"endpoints": []}

        mgr = _make_manager(mock_vsc)
        mgr.sync_index()

        mock_index.sync.assert_called_once()


# ---------------------------------------------------------------------------
# process_chunks tests
# ---------------------------------------------------------------------------


class TestProcessChunks:
    """Tests for the process_chunks method (all deps mocked)."""

    @patch.object(VectorSearchManager, "read_and_chunk")
    @patch.object(VectorSearchManager, "get_unprocessed_files")
    @patch.object(VectorSearchManager, "create_chunks_table")
    def test_chunks_and_writes(
        self,
        mock_create: MagicMock,
        mock_get: MagicMock,
        mock_chunk: MagicMock,
    ) -> None:
        mock_spark = MagicMock()
        mock_get.return_value = [
            {
                "guid": "g1",
                "volume_path": "/Volumes/c/s/v/blog/post.txt",
                "source": "blog",
                "title": "Post",
            },
        ]
        mock_chunk.return_value = ["chunk 1", "chunk 2"]

        mgr = _make_manager()
        count = mgr.process_chunks(spark=mock_spark)

        assert count == 2
        mock_create.assert_called_once()
        mock_chunk.assert_called_once()
        mock_spark.createDataFrame.assert_called_once()
        mock_spark.createDataFrame.return_value.write.format.return_value.mode.return_value.saveAsTable.assert_called_once()

    @patch.object(VectorSearchManager, "get_unprocessed_files")
    @patch.object(VectorSearchManager, "create_chunks_table")
    def test_returns_zero_when_nothing_to_process(
        self,
        mock_create: MagicMock,
        mock_get: MagicMock,
    ) -> None:
        mock_spark = MagicMock()
        mock_get.return_value = []

        mgr = _make_manager()
        count = mgr.process_chunks(spark=mock_spark)
        assert count == 0

    @patch.object(VectorSearchManager, "read_and_chunk")
    @patch.object(VectorSearchManager, "get_unprocessed_files")
    @patch.object(VectorSearchManager, "create_chunks_table")
    def test_continues_on_read_failure(
        self,
        mock_create: MagicMock,
        mock_get: MagicMock,
        mock_chunk: MagicMock,
    ) -> None:
        mock_spark = MagicMock()
        mock_get.return_value = [
            {
                "guid": "g1",
                "volume_path": "/Volumes/c/s/v/blog/fail.txt",
                "source": "blog",
                "title": "Fail",
            },
            {
                "guid": "g2",
                "volume_path": "/Volumes/c/s/v/blog/ok.txt",
                "source": "blog",
                "title": "OK",
            },
        ]
        mock_chunk.side_effect = [RuntimeError("read error"), ["chunk"]]

        mgr = _make_manager()
        count = mgr.process_chunks(spark=mock_spark)

        assert count == 1

    @patch.object(VectorSearchManager, "read_and_chunk")
    @patch.object(VectorSearchManager, "get_unprocessed_files")
    @patch.object(VectorSearchManager, "create_chunks_table")
    def test_chunk_ids_are_deterministic(
        self,
        mock_create: MagicMock,
        mock_get: MagicMock,
        mock_chunk: MagicMock,
    ) -> None:
        mock_spark = MagicMock()
        mock_get.return_value = [
            {
                "guid": "g1",
                "volume_path": "/Volumes/c/s/v/blog/post.txt",
                "source": "blog",
                "title": "Post",
            },
        ]
        mock_chunk.return_value = ["chunk"]

        mgr = _make_manager()
        mgr.process_chunks(spark=mock_spark)

        rows = mock_spark.createDataFrame.call_args[0][0]
        expected_id = hashlib.sha256(b"g1:0").hexdigest()
        assert rows[0]["chunk_id"] == expected_id

    @patch.object(VectorSearchManager, "read_and_chunk")
    @patch.object(VectorSearchManager, "get_unprocessed_files")
    @patch.object(VectorSearchManager, "create_chunks_table")
    def test_uses_explicit_schema(
        self,
        mock_create: MagicMock,
        mock_get: MagicMock,
        mock_chunk: MagicMock,
    ) -> None:
        mock_spark = MagicMock()
        mock_get.return_value = [
            {
                "guid": "g1",
                "volume_path": "/v/p.txt",
                "source": "blog",
                "title": "P",
            },
        ]
        mock_chunk.return_value = ["chunk"]

        mgr = _make_manager()
        mgr.process_chunks(spark=mock_spark)

        _, kwargs = mock_spark.createDataFrame.call_args
        assert kwargs["schema"] is VectorSearchManager.SCHEMA


# ---------------------------------------------------------------------------
# embed_files tests
# ---------------------------------------------------------------------------


class TestEmbedFiles:
    """Tests for the embed_files orchestration method."""

    @patch.object(VectorSearchManager, "sync_index")
    @patch.object(VectorSearchManager, "process_chunks", return_value=5)
    def test_syncs_index_after_chunking(
        self,
        mock_process: MagicMock,
        mock_sync: MagicMock,
    ) -> None:
        mock_spark = MagicMock()
        mgr = _make_manager()
        count = mgr.embed_files(spark=mock_spark)

        assert count == 5
        mock_process.assert_called_once_with(spark=mock_spark)
        mock_sync.assert_called_once()

    @patch.object(VectorSearchManager, "sync_index")
    @patch.object(VectorSearchManager, "process_chunks", return_value=0)
    def test_syncs_index_even_with_zero_chunks(
        self,
        mock_process: MagicMock,
        mock_sync: MagicMock,
    ) -> None:
        """Current code uses ``>= 0`` so sync is always called."""
        mock_spark = MagicMock()
        mgr = _make_manager()
        count = mgr.embed_files(spark=mock_spark)

        assert count == 0
        mock_sync.assert_called_once()


# ---------------------------------------------------------------------------
# Search tests
# ---------------------------------------------------------------------------


class TestSearch:
    """Tests for the search method."""

    def test_search_calls_similarity_search(self) -> None:
        mock_vsc = MagicMock()
        mock_index = MagicMock()
        mock_vsc.get_index.return_value = mock_index
        mock_index.similarity_search.return_value = {
            "result": {"data_array": [["id1", "text1"]]},
        }

        mgr = _make_manager(mock_vsc)
        results = mgr.search("test query")

        mock_index.similarity_search.assert_called_once()
        kw = mock_index.similarity_search.call_args[1]
        assert kw["query_text"] == "test query"
        assert kw["num_results"] == 5
        assert kw["columns"] == list(_SYNC_COLUMNS)
        assert results["result"]["data_array"] == [["id1", "text1"]]

    def test_search_with_custom_params(self) -> None:
        mock_vsc = MagicMock()
        mock_index = MagicMock()
        mock_vsc.get_index.return_value = mock_index
        mock_index.similarity_search.return_value = {
            "result": {"data_array": []},
        }

        mgr = _make_manager(mock_vsc)
        mgr.search(
            "query",
            num_results=10,
            filters={"source_label": "blog"},
            columns=["chunk_text", "title"],
        )

        kw = mock_index.similarity_search.call_args[1]
        assert kw["num_results"] == 10
        assert kw["filters"] == {"source_label": "blog"}
        assert kw["columns"] == ["chunk_text", "title"]
