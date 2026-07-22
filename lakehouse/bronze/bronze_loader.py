"""Bronze layer: raw document landing.

Reads the validated-event manifest produced by Stage 1
(`data/bronze_manifest/validated_events.jsonl`), extracts the raw text of
each document, and lands it as-is — untransformed, minimally typed — into
a Delta table. Bronze is rebuilt from the full manifest each run (the
manifest is already the validated, deduplicated source of truth), so this
uses `overwrite` rather than append; Bronze is still schema-enforced, so a
malformed row can never silently corrupt the table.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from configs.spark_session import get_spark_session  # noqa: E402
from utils.document_loader import extract_text  # noqa: E402

MANIFEST_PATH = PROJECT_ROOT / "data" / "bronze_manifest" / "validated_events.jsonl"
BRONZE_TABLE_PATH = str(PROJECT_ROOT / "lakehouse" / "bronze" / "documents_bronze")
WAREHOUSE_DIR = str(PROJECT_ROOT / "lakehouse" / "_spark_warehouse")

BRONZE_SCHEMA = StructType(
    [
        StructField("document_id", StringType(), nullable=False),
        StructField("file_name", StringType(), nullable=False),
        StructField("file_path", StringType(), nullable=False),
        StructField("file_type", StringType(), nullable=False),
        StructField("file_size_bytes", LongType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("course_code", StringType(), nullable=True),
        StructField("uploaded_by", StringType(), nullable=False),
        StructField("upload_timestamp", StringType(), nullable=False),
        StructField("raw_text", StringType(), nullable=True),
        StructField("bronze_ingested_at", TimestampType(), nullable=False),
    ]
)


def read_manifest() -> list[dict]:
    if not MANIFEST_PATH.is_file():
        raise FileNotFoundError(
            f"No validated manifest at {MANIFEST_PATH} — run Stage 1 "
            "(producer + consumer) first."
        )
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def build_bronze_rows(events: list[dict]) -> list[tuple]:
    now = datetime.now(timezone.utc)
    rows = []
    for event in events:
        try:
            raw_text = extract_text(event["file_path"], event["file_type"])
        except Exception as exc:
            logger.warning(f"Could not extract text for {event['file_name']}: {exc}")
            raw_text = None
        rows.append(
            (
                event["document_id"],
                event["file_name"],
                event["file_path"],
                event["file_type"],
                int(event["file_size_bytes"]),
                event.get("category"),
                event.get("course_code"),
                event["uploaded_by"],
                event["upload_timestamp"],
                raw_text,
                now,
            )
        )
    return rows


def main() -> dict:
    logger.add(
        PROJECT_ROOT / "logs" / "bronze_loader.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="1 MB",
    )

    events = read_manifest()
    logger.info(f"Loaded {len(events)} validated events from manifest")

    rows = build_bronze_rows(events)
    extracted = sum(1 for r in rows if r[9] is not None)
    logger.info(f"Extracted raw text for {extracted}/{len(rows)} documents")

    spark = get_spark_session("BronzeLoader", WAREHOUSE_DIR)
    df = spark.createDataFrame(rows, BRONZE_SCHEMA)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_TABLE_PATH)
    )
    row_count = df.count()
    logger.success(f"Bronze table written: {row_count} rows -> {BRONZE_TABLE_PATH}")

    spark.read.format("delta").load(BRONZE_TABLE_PATH).select(
        "document_id", "file_name", "file_type", "category", "file_size_bytes"
    ).show(truncate=False)

    spark.stop()
    return {"row_count": row_count, "extracted_text_count": extracted}


if __name__ == "__main__":
    main()
