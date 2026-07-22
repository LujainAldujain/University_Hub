"""Demonstrates the Great Expectations quality gate actually halting the
pipeline — not just the happy path.

Deliberately injects a duplicate `file_name` into Silver (the exact
business-key uniqueness violation a buggy MERGE could produce), then runs
the same quality_checkpoint -> gold_aggregate sequence the Airflow DAG and
scripts/run_pipeline_with_lineage.py use. Proves three things at once:

  1. The GX checkpoint actually detects the corruption (uniqueness
     expectation fails).
  2. A real OpenLineage FAIL event is emitted for quality_checkpoint.
  3. gold_aggregate is never called — the gate genuinely blocks it.

Cleans up the injected row afterward so Silver is left in its correct state.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from lakehouse.gold.gold_aggregate import main as gold_main  # noqa: E402
from lineage.lineage_emitter import emit_lineage  # noqa: E402
from quality.ge_checkpoint import main as run_quality_checkpoint  # noqa: E402

SILVER_TABLE_PATH = str(PROJECT_ROOT / "lakehouse" / "silver" / "documents_silver")
INJECTED_ID = "duplicate-test-0000-0000-000000000000"


def inject_duplicate_file_name() -> None:
    dt = DeltaTable(SILVER_TABLE_PATH)
    tbl = dt.to_pyarrow_table()
    row = tbl.to_pylist()[0]
    row["document_id"] = INJECTED_ID
    dup_table = pa.Table.from_pylist([row], schema=tbl.schema)
    write_deltalake(SILVER_TABLE_PATH, dup_table, mode="append")
    logger.warning(
        f"Injected a duplicate file_name={row['file_name']!r} into Silver "
        f"(document_id={INJECTED_ID}) to simulate a MERGE bug"
    )


def cleanup_injected_row() -> None:
    dt = DeltaTable(SILVER_TABLE_PATH)
    dt.delete(predicate=f"document_id = '{INJECTED_ID}'")
    logger.info("Cleaned up the injected row — Silver restored to its correct state")


def main() -> None:
    logger.add(
        PROJECT_ROOT / "logs" / "quality_gate_failure_demo.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="1 MB",
    )

    inject_duplicate_file_name()

    gold_was_called = False
    try:
        with emit_lineage("quality_checkpoint"):
            passed = run_quality_checkpoint()
            if not passed:
                raise RuntimeError(
                    "Great Expectations quality gate FAILED on Silver — "
                    "halting before Gold."
                )

        with emit_lineage("gold_aggregate"):
            gold_was_called = True
            gold_main()

    except RuntimeError as exc:
        logger.error(f"PIPELINE HALTED: {exc}")

    logger.info(f"gold_aggregate was called: {gold_was_called} (must be False)")
    assert gold_was_called is False, "Gate failed to block Gold — this is a real bug"

    cleanup_injected_row()
    logger.success("Failure-path demo complete: gate blocked Gold, then Silver was restored")


if __name__ == "__main__":
    main()
