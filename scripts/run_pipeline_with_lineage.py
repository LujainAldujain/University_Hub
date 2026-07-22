"""Runs the full pipeline end-to-end (ingestion -> Bronze -> Silver ->
quality gate -> Gold), wrapping every stage in real OpenLineage
START/COMPLETE/FAIL events, with two real gates that halt downstream
stages on failure:

  1. Ingestion gate — if zero documents pass the Pydantic contract,
     Bronze/Silver/Gold never run (same gate the Airflow DAG uses).
  2. Great Expectations gate — if the Silver quality checkpoint fails,
     Gold never runs.

This is the local (non-Airflow) equivalent of the DAG in
airflow/dags/university_pipeline_dag.py, for evidence captured on a
machine where Airflow itself can't run (see README "Stage 4").
"""
from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from consumer.document_consumer import main as consume  # noqa: E402
from lakehouse.bronze.bronze_loader import main as bronze_main  # noqa: E402
from lakehouse.gold.gold_aggregate import main as gold_main  # noqa: E402
from lakehouse.silver.silver_transform import main as silver_main  # noqa: E402
from lineage.lineage_emitter import emit_lineage  # noqa: E402
from producer.document_producer import main as produce  # noqa: E402
from quality.ge_checkpoint import main as run_quality_checkpoint  # noqa: E402

MIN_ACCEPTED_DOCUMENTS = 1


class PipelineHalted(Exception):
    """Raised when a gate fails and the pipeline must stop before a stage."""


def main() -> None:
    logger.add(
        PROJECT_ROOT / "logs" / "pipeline_with_lineage.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="1 MB",
    )

    with emit_lineage("produce_documents"):
        produce()

    with emit_lineage("validate_and_consume"):
        consume_result = consume()

    with emit_lineage("check_ingestion_quality"):
        accepted = consume_result["accepted"]
        if accepted < MIN_ACCEPTED_DOCUMENTS:
            raise PipelineHalted(
                f"Ingestion gate FAILED: only {accepted} document(s) accepted "
                f"({consume_result['rejected']} rejected) — halting before Bronze."
            )
        logger.success(f"Ingestion gate passed: {accepted} document(s) accepted")

    with emit_lineage("bronze_load"):
        bronze_main()

    with emit_lineage("silver_transform"):
        silver_main()

    with emit_lineage("quality_checkpoint"):
        quality_passed = run_quality_checkpoint()
        if not quality_passed:
            raise PipelineHalted(
                "Great Expectations quality gate FAILED on Silver — halting "
                "before Gold. See logs/ge_checkpoint.log for which "
                "expectation(s) failed."
            )

    with emit_lineage("gold_aggregate"):
        gold_main()

    logger.success("Full pipeline completed: ingestion -> Bronze -> Silver -> quality gate -> Gold")


if __name__ == "__main__":
    try:
        main()
    except PipelineHalted as exc:
        logger.error(f"PIPELINE HALTED: {exc}")
        sys.exit(1)
