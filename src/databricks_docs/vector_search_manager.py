"""
Manage Databricks Vector Search:
table setup, chunking, embedding, and index setup.
"""

from __future__ import annotations

import datetime
import hashlib
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
from databricks.vector_search.index import VectorSearchIndex
from langchain_text_splitters import RecursiveCharacterTextSplitter
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks_docs.config import IngestionConfig

# Columns synced into the Vector Search index for filtering / retrieval.
_SYNC_COLUMNS = [
    "chunk_id",
    "chunk_text",
    "title",
]


class VectorSearchManager:
    """Lifecycle manager for Vector Search: table setup, chunking, embedding,
    endpoint / index management, and search.

    Parameters
    ----------
    config_path
        Path to the YAML configuration file.
    """

    SCHEMA = StructType(
        [
            StructField("chunk_id", StringType(), nullable=False),
            StructField("source_guid", StringType(), nullable=False),
            StructField("source_label", StringType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("chunk_index", IntegerType(), nullable=False),
            StructField("chunk_text", StringType(), nullable=False),
            StructField("chunked_at", TimestampType(), nullable=False),
        ]
    )

    def __init__(
        self,
        local: bool,
        config_path: str | Path,
        workspace_client: WorkspaceClient | None,
    ) -> None:
        self.config = IngestionConfig.load(config_path)

        self.workspace_client = workspace_client

        self.local = local
        if self.local:
            assert isinstance(self.workspace_client, WorkspaceClient)
            self.vs_client = VectorSearchClient(
                workspace_url=self.workspace_client.config.host,
                personal_access_token=self.workspace_client.tokens.create(
                    lifetime_seconds=1200
                ).token_value,
            )
        else:
            self.vs_client = VectorSearchClient()

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    def create_chunks_table(self, spark: SparkSession) -> None:
        """Create the chunks Delta table if it doesn't exist."""
        fqn = self.config.full_chunks_table_name
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self.config.catalog}.{self.config.schema_name}"
        )
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {fqn} (
                chunk_id STRING NOT NULL,
                source_guid STRING NOT NULL,
                source_label STRING NOT NULL,
                title STRING NOT NULL,
                chunk_index INT NOT NULL,
                chunk_text STRING NOT NULL,
                chunked_at TIMESTAMP NOT NULL
            )
            USING DELTA
            TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
        logger.info("Ensured chunks table exists: {}", fqn)

    # ------------------------------------------------------------------
    # Chunking helpers
    # ------------------------------------------------------------------

    def get_unprocessed_files(self, spark: SparkSession) -> list:
        """Return processed feed items that have not yet been chunked."""
        rows = spark.sql(f"""
            SELECT fi.guid, fi.volume_path, fi.source, fi.title
            FROM {self.config.full_table_name} fi
            LEFT ANTI JOIN {self.config.full_chunks_table_name} c
                ON fi.guid = c.source_guid
            WHERE fi.processed = true
              AND fi.volume_path IS NOT NULL
        """).collect()
        logger.info("Found {} files to embed", len(rows))
        return rows

    def read_and_chunk(
        self,
        volume_path: str,
    ) -> list[str]:
        """Read a text file from the Volume and split into chunks."""
        if self.local:
            assert isinstance(self.workspace_client, WorkspaceClient)
            response = self.workspace_client.files.download(volume_path)
            text = response.contents.read().decode("utf-8")
        else:
            with open(volume_path, encoding="utf-8") as f:
                text = f.read()

        if not text.strip():
            return []

        splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.config.chunk_size,
            chunk_overlap=self.config.chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", ". ", "? ", "! ", " ", ""],
        )
        return splitter.split_text(text)

    # ------------------------------------------------------------------
    # Endpoint management
    # ------------------------------------------------------------------

    def create_endpoint_if_not_exists(self) -> None:
        """Create vector search endpoint if it doesn't exist."""
        endpoints_response = self.vs_client.list_endpoints()
        endpoints = (
            endpoints_response.get("endpoints", [])
            if isinstance(endpoints_response, dict)
            else []
        )
        endpoint_exists = any(
            (ep.get("name") if isinstance(ep, dict) else getattr(ep, "name", None))
            == self.config.vector_search_endpoint_name
            for ep in endpoints
        )

        if not endpoint_exists:
            logger.info(
                "Creating vector search endpoint: "
                + f"{self.config.vector_search_endpoint_name}"
            )
            self.vs_client.create_endpoint_and_wait(
                name=self.config.vector_search_endpoint_name,
                endpoint_type="STANDARD",
            )
            logger.info(
                "✓ Vector search endpoint created: "
                + f"{self.config.vector_search_endpoint_name}"
            )
        else:
            logger.info(
                "✓ Vector search endpoint exists: "
                + f"{self.config.vector_search_endpoint_name}"
            )

    # ------------------------------------------------------------------
    # Index management
    # ------------------------------------------------------------------

    def create_or_get_index(self) -> VectorSearchIndex:
        """Create or get vector search index.

        Returns:
            Vector search index object
        """
        self.create_endpoint_if_not_exists()

        # Try to get existing index
        try:
            index = self.vs_client.get_index(
                endpoint_name=self.config.vector_search_endpoint_name,
                index_name=self.config.full_index_name,
            )
            logger.info(f"✓ Vector search index exists: {self.config.full_index_name}")
            return index
        except Exception:
            logger.info(f"Index {self.config.full_index_name} not found, will create it")

        # Try to create the index
        try:
            index = self.vs_client.create_delta_sync_index(
                endpoint_name=self.config.vector_search_endpoint_name,
                source_table_name=self.config.full_chunks_table_name,
                index_name=self.config.full_index_name,
                pipeline_type="TRIGGERED",
                primary_key="chunk_id",
                embedding_source_column="chunk_text",
                embedding_model_endpoint_name=self.config.embedding_model,
            )
            logger.info(f"✓ Vector search index created: {self.config.full_index_name}")
            return index
        except Exception as e:
            if "RESOURCE_ALREADY_EXISTS" not in str(e):
                raise
            # Index exists but get_index failed earlier (transient) — retry
            logger.info(f"✓ Vector search index exists: {self.config.full_index_name}")
            return self.vs_client.get_index(
                endpoint_name=self.config.vector_search_endpoint_name,
                index_name=self.config.full_index_name,
            )

    def sync_index(self) -> None:
        """Sync the vector search index with the source table."""
        index = self.create_or_get_index()
        logger.info(f"Syncing vector search index: {self.config.full_index_name}")
        index.sync()
        logger.info("✓ Index sync triggered")

    # ------------------------------------------------------------------
    # Embedding orchestration
    # ------------------------------------------------------------------

    def process_chunks(
        self,
        spark: SparkSession,
    ) -> int:
        """Chunk files and write them to the Delta table.

        Returns the total number of chunks written.
        """
        self.create_chunks_table(spark)
        rows = self.get_unprocessed_files(spark)

        if not rows:
            logger.info("No files to embed.")
            return 0

        total_chunks = 0
        for row in rows:
            guid: str = row["guid"]
            volume_path: str = row["volume_path"]
            source: str = row["source"]
            title: str = row["title"]

            logger.info("Chunking [{}/{}]: {}", source, title, volume_path)

            try:
                chunks = self.read_and_chunk(volume_path=volume_path)
            except Exception:
                logger.exception("Failed to read/chunk {}", volume_path)
                continue

            if not chunks:
                logger.warning("No chunks produced for {}", volume_path)
                continue

            now = datetime.datetime.now(tz=datetime.UTC)
            chunk_rows = [
                {
                    "chunk_id": hashlib.sha256(f"{guid}:{idx}".encode()).hexdigest(),
                    "source_guid": guid,
                    "source_label": source,
                    "title": title,
                    "chunk_index": idx,
                    "chunk_text": chunk_text,
                    "chunked_at": now,
                }
                for idx, chunk_text in enumerate(chunks)
            ]

            df = spark.createDataFrame(chunk_rows, schema=self.SCHEMA)
            df.write.format("delta").mode("append").saveAsTable(
                self.config.full_chunks_table_name,
            )
            total_chunks += len(chunk_rows)
            logger.info("Wrote {} chunks for {}", len(chunk_rows), title)

        logger.info(
            "Chunking complete. {} total chunks from {} files.",
            total_chunks,
            len(rows),
        )
        return total_chunks

    def embed_files(self, spark: SparkSession) -> int:
        """Orchestrate the full embedding process: chunking, writing to Delta,
        and syncing the index. Returns the number of chunks processed."""
        chunk_count = self.process_chunks(spark=spark)

        if chunk_count >= 0:
            logger.info("Creating or syncing vector search index...")

            self.sync_index()

            logger.info("Vector search index sync triggered successfully.")

        return chunk_count

    # ------------------------------------------------------------------
    # Vector Search
    # ------------------------------------------------------------------

    def search(
        self,
        query_text: str,
        num_results: int = 5,
        filters: dict | None = None,
        columns: list[str] | None = None,
    ) -> dict:
        """Run a similarity search against the Vector Search index."""
        if columns is None:
            columns = list(_SYNC_COLUMNS)

        idx = self.vs_client.get_index(
            endpoint_name=self.config.vector_search_endpoint_name,
            index_name=self.config.full_index_name,
        )
        results = idx.similarity_search(
            query_text=query_text,
            columns=columns,
            num_results=num_results,
            filters=filters,
        )
        logger.info(
            "Search returned {} results for '{}...'",
            len(results.get("result", {}).get("data_array", [])),
            query_text[:80],
        )
        return results
