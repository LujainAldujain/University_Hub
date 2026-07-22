"""Gold layer: analytics-ready knowledge-base rollups.

A genuine aggregate over Silver — per-category document statistics — not a
row-for-row copy. Recomputed (overwritten) from Silver each run, since an
aggregate is naturally idempotent to fully recompute; the real MERGE/upsert
requirement is demonstrated once, in Silver, on the true business key.
"""
from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from configs.spark_session import get_spark_session  # noqa: E402

SILVER_TABLE_PATH = str(PROJECT_ROOT / "lakehouse" / "silver" / "documents_silver")
GOLD_TABLE_PATH = str(PROJECT_ROOT / "lakehouse" / "gold" / "category_knowledge_stats")
WAREHOUSE_DIR = str(PROJECT_ROOT / "lakehouse" / "_spark_warehouse")


def main() -> dict:
    logger.add(
        PROJECT_ROOT / "logs" / "gold_aggregate.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="1 MB",
    )

    spark = get_spark_session("GoldAggregate", WAREHOUSE_DIR)

    silver = spark.read.format("delta").load(SILVER_TABLE_PATH).filter("is_valid = true")
    silver_row_count = silver.count()
    logger.info(f"Read {silver_row_count} valid rows from Silver")

    gold = (
        silver.groupBy("category")
        .agg(
            F.count("*").alias("document_count"),
            F.sum("word_count").alias("total_word_count"),
            F.round(F.avg("word_count"), 1).alias("avg_word_count"),
            F.countDistinct("uploaded_by").alias("distinct_uploaders"),
            F.max("silver_updated_at").alias("last_updated_at"),
        )
        .orderBy("category")
    )

    (
        gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_TABLE_PATH)
    )

    from delta.tables import DeltaTable

    DeltaTable.forPath(spark, GOLD_TABLE_PATH).optimize().executeCompaction()

    gold_row_count = spark.read.format("delta").load(GOLD_TABLE_PATH).count()
    logger.success(
        f"Gold table written + OPTIMIZEd: {gold_row_count} category rows "
        f"(aggregated from {silver_row_count} Silver document rows) -> {GOLD_TABLE_PATH}"
    )
    logger.info(
        f"Confirms Gold is a genuine aggregate, not a copy of Silver: "
        f"{silver_row_count} document rows -> {gold_row_count} category rows, "
        f"different schema (rollup columns vs per-document columns)"
    )

    spark.read.format("delta").load(GOLD_TABLE_PATH).show(truncate=False)

    spark.stop()
    return {"silver_row_count": silver_row_count, "gold_row_count": gold_row_count}


if __name__ == "__main__":
    main()
