"""Real Great Expectations 1.x checkpoint gating the Lakehouse pipeline.

Runs against the governed Silver table (not Bronze) — this is a distinct
quality layer from Stage 1's Pydantic ingestion contract: it verifies the
*state* of the table Bronze/Silver actually produced, catching problems a
per-record contract can't see, most importantly whether the Stage 2 MERGE
kept exactly one row per business key (`file_name`).

If this checkpoint fails, the caller (scripts/run_pipeline_with_lineage.py,
and the `quality_checkpoint` Airflow task once wired) must not proceed to
Gold — this module only reports pass/fail, the gating decision is the
caller's responsibility, matching how Stage 1's ingestion gate works.
"""
from __future__ import annotations

import sys
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
from deltalake import DeltaTable
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

SILVER_TABLE_PATH = str(PROJECT_ROOT / "lakehouse" / "silver" / "documents_silver")


def load_silver_as_pandas():
    dt = DeltaTable(SILVER_TABLE_PATH)
    return dt.to_pyarrow_table().to_pandas()


def run_silver_quality_checkpoint(df=None) -> dict:
    """Runs the real GX checkpoint. Returns {"success": bool, "results": [...]}."""
    if df is None:
        df = load_silver_as_pandas()

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("pandas_silver")
    data_asset = data_source.add_dataframe_asset(name="silver_documents")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("whole_df")

    suite = context.suites.add(gx.ExpectationSuite(name="silver_quality_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="document_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="file_name"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToMatchRegex(column="file_type", regex=r"^(pdf|docx|txt)$")
    )
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="word_count", min_value=1))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="file_name"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="is_valid", value_set=[True])
    )

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="silver_quality_validation", data=batch_definition, suite=suite
        )
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="silver_quality_checkpoint", validation_definitions=[validation_definition]
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})

    details = []
    for run_result in result.run_results.values():
        for r in run_result["results"]:
            details.append(
                {
                    "expectation": r["expectation_config"]["type"],
                    "column": r["expectation_config"]["kwargs"].get("column"),
                    "success": r["success"],
                }
            )

    return {"success": result.success, "results": details, "row_count": len(df)}


def main() -> bool:
    logger.add(
        PROJECT_ROOT / "logs" / "ge_checkpoint.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="1 MB",
    )

    outcome = run_silver_quality_checkpoint()

    logger.info(f"Great Expectations checkpoint against Silver ({outcome['row_count']} rows):")
    for r in outcome["results"]:
        status = "PASSED" if r["success"] else "FAILED"
        icon_log = logger.success if r["success"] else logger.error
        icon_log(f"  [{status}] {r['expectation']} (column={r['column']})")

    if outcome["success"]:
        logger.success("Quality gate PASSED — safe to proceed to Gold")
    else:
        logger.error("Quality gate FAILED — pipeline must halt before Gold")

    return outcome["success"]


if __name__ == "__main__":
    passed = main()
    sys.exit(0 if passed else 1)
