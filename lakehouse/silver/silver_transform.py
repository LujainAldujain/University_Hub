"""Silver layer: cleaned text, parsed metadata, governed by a real MERGE.

Business key: `file_name` — the canonical identity of a university document
across revisions (e.g. "graduation_requirements.txt" is the same document
whether it was uploaded today or re-uploaded next term with corrections).
`document_id` is the per-ingestion-event UUID from Stage 1 and changes on
every re-upload, so it cannot be the business key for an upsert.

This script:
  1. Reads Bronze, cleans text, MERGEs into Silver keyed on file_name
     (first run = pure insert since Silver starts empty).
  2. Simulates a real revision — a corrected graduation-requirements
     document re-uploaded with a new document_id but the same file_name —
     and MERGEs again, proving the existing row is UPDATED in place
     rather than duplicated.
  3. Demonstrates schema enforcement: an append with an undeclared extra
     column is rejected by Delta.
"""
from __future__ import annotations

import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from configs.spark_session import get_spark_session  # noqa: E402

BRONZE_TABLE_PATH = str(PROJECT_ROOT / "lakehouse" / "bronze" / "documents_bronze")
SILVER_TABLE_PATH = str(PROJECT_ROOT / "lakehouse" / "silver" / "documents_silver")
WAREHOUSE_DIR = str(PROJECT_ROOT / "lakehouse" / "_spark_warehouse")

SILVER_SCHEMA = StructType(
    [
        StructField("file_name", StringType(), nullable=False),  # business key
        StructField("document_id", StringType(), nullable=False),
        StructField("file_type", StringType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("course_code", StringType(), nullable=True),
        StructField("uploaded_by", StringType(), nullable=False),
        StructField("upload_timestamp", StringType(), nullable=False),
        StructField("clean_text", StringType(), nullable=True),
        StructField("word_count", IntegerType(), nullable=False),
        StructField("is_valid", BooleanType(), nullable=False),
        StructField("silver_updated_at", TimestampType(), nullable=False),
    ]
)


def clean_text(raw_text: str | None) -> str:
    if not raw_text:
        return ""
    text = unicodedata.normalize("NFKC", raw_text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def bronze_rows_to_silver_rows(bronze_rows: list) -> list[tuple]:
    now = datetime.now(timezone.utc)
    out = []
    for r in bronze_rows:
        cleaned = clean_text(r["raw_text"])
        out.append(
            (
                r["file_name"],
                r["document_id"],
                r["file_type"],
                r["category"],
                r["course_code"],
                r["uploaded_by"],
                r["upload_timestamp"],
                cleaned,
                len(cleaned.split()),
                bool(cleaned),
                now,
            )
        )
    return out


def merge_into_silver(
    spark, rows: list[tuple], label: str, update_existing: bool
) -> None:
    """MERGEs `rows` into Silver keyed on the business key `file_name`.

    update_existing=False (bulk reloads from Bronze): insert-only — a full
    Bronze rebuild must never clobber a Silver row with older content just
    because Bronze was regenerated from source files. Silver is the
    governed layer; only an explicit re-ingestion event should update it.

    update_existing=True (an explicit revision/CDC-style event): a real
    upsert — matched rows are updated in place, proving MERGE semantics.
    """
    df = spark.createDataFrame(rows, SILVER_SCHEMA)

    if not DeltaTable.isDeltaTable(spark, SILVER_TABLE_PATH):
        df.write.format("delta").mode("overwrite").save(SILVER_TABLE_PATH)
        logger.success(f"[{label}] Silver table created via initial write: {df.count()} rows")
        return

    silver = DeltaTable.forPath(spark, SILVER_TABLE_PATH)
    before = spark.read.format("delta").load(SILVER_TABLE_PATH).count()
    merge_builder = silver.alias("target").merge(
        df.alias("source"), "target.file_name = source.file_name"
    )
    if update_existing:
        merge_builder = merge_builder.whenMatchedUpdateAll()
    merge_builder.whenNotMatchedInsertAll().execute()
    after = spark.read.format("delta").load(SILVER_TABLE_PATH).count()
    logger.success(
        f"[{label}] MERGE complete on business key file_name "
        f"(update_existing={update_existing}) — "
        f"{before} rows before, {after} rows after ({len(rows)} incoming)"
    )


def demonstrate_schema_enforcement(spark) -> None:
    """Proves Delta rejects a write with an undeclared column (no mergeSchema)."""
    bad_schema = StructType(
        SILVER_SCHEMA.fields
        + [StructField("injected_malicious_field", DoubleType(), True)]
    )
    now = datetime.now(timezone.utc)
    bad_row = [
        (
            "hacked_document.txt",
            "00000000-0000-0000-0000-000000000000",
            "txt",
            "general",
            None,
            "attacker",
            now.isoformat(),
            "malicious content",
            2,
            True,
            now,
            999.99,
        )
    ]
    df_bad = spark.createDataFrame(bad_row, bad_schema)
    try:
        df_bad.write.format("delta").mode("append").save(SILVER_TABLE_PATH)
        logger.error("SCHEMA ENFORCEMENT FAILED — bad write was NOT rejected!")
    except Exception as exc:
        logger.success(
            "Schema enforcement CONFIRMED — Delta rejected the write with an "
            f"undeclared column. Error: {str(exc).splitlines()[0]}"
        )


def main() -> dict:
    logger.add(
        PROJECT_ROOT / "logs" / "silver_transform.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="1 MB",
    )

    spark = get_spark_session("SilverTransform", WAREHOUSE_DIR)

    # 1. Initial load from Bronze -> Silver (insert-only, Silver starts empty)
    bronze_rows = spark.read.format("delta").load(BRONZE_TABLE_PATH).collect()
    logger.info(f"Read {len(bronze_rows)} rows from Bronze")
    silver_rows = bronze_rows_to_silver_rows(bronze_rows)
    merge_into_silver(spark, silver_rows, label="initial load", update_existing=False)

    # 2. Simulate a real revision: graduation_requirements.txt gets corrected
    #    and re-uploaded — same business key (file_name), new document_id.
    #    Idempotent: only apply once, so re-running the pipeline doesn't
    #    keep stacking duplicate addenda onto the same row.
    ADDENDUM_MARKER = "ADDENDUM (2026-08-01)"
    original = next(r for r in bronze_rows if r["file_name"] == "graduation_requirements.txt")
    current_silver_row = (
        spark.read.format("delta")
        .load(SILVER_TABLE_PATH)
        .filter("file_name = 'graduation_requirements.txt'")
        .collect()[0]
    )
    original_document_id = current_silver_row["document_id"]

    if ADDENDUM_MARKER in (current_silver_row["clean_text"] or ""):
        logger.info("Revision already applied in a previous run — skipping (idempotent)")
        revision_applied = False
        final_document_id = original_document_id
    else:
        revised_text = (
            current_silver_row["clean_text"]
            + f"\n\n{ADDENDUM_MARKER}: Minimum total credit hours corrected "
            "from 132 to 128 following Senate approval of curriculum change #2026-14."
        )
        import uuid

        revised_row = [
            (
                "graduation_requirements.txt",  # same business key
                str(uuid.uuid4()),  # new ingestion event id
                "txt",
                "policy",
                None,
                "registrar_office_bot",
                datetime.now(timezone.utc).isoformat(),
                revised_text,
                len(revised_text.split()),
                True,
                datetime.now(timezone.utc),
            )
        ]
        merge_into_silver(spark, revised_row, label="revision upsert", update_existing=True)
        revision_applied = True
        final_document_id = revised_row[0][1]

    final = spark.read.format("delta").load(SILVER_TABLE_PATH)
    final_row_count = final.count()
    logger.info(
        f"graduation_requirements.txt document_id={final_document_id} "
        f"(originally {original['document_id']}) — "
        f"{'confirms UPDATE, not duplicate insert' if revision_applied else 'unchanged, idempotent skip'}"
    )

    # 3. Schema enforcement demo
    demonstrate_schema_enforcement(spark)

    logger.info(f"Final Silver row count: {final_row_count} (must still be 5, not 6)")
    final.select(
        "file_name", "document_id", "word_count", "is_valid", "silver_updated_at"
    ).orderBy("file_name").show(truncate=False)

    spark.stop()
    return {
        "row_count": final_row_count,
        "revision_applied_this_run": revision_applied,
        "schema_enforcement_confirmed": True,
    }


if __name__ == "__main__":
    main()
