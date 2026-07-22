"""AI University Knowledge Hub — full pipeline orchestration DAG.

Wires together every stage with correct task dependencies:

    produce_documents -> validate_and_consume -> check_ingestion_quality
        -> bronze_load -> silver_transform -> quality_checkpoint
        -> gold_aggregate

Two real gates halt downstream tasks on failure:

  - `check_ingestion_quality` inspects the real accept/reject counts the
    Kafka consumer returns (via XCom) and fails if nothing passed the
    Pydantic contract.
  - `quality_checkpoint` runs a real Great Expectations checkpoint against
    the governed Silver table and fails if it doesn't pass (e.g. a
    business-key uniqueness violation, meaning the Stage 2 MERGE didn't do
    its job).

Because every downstream task depends on its gate through TaskFlow's
argument-passing (which Airflow turns into real upstream/downstream edges)
with the default `all_success` trigger rule, a failed gate leaves the
remaining tasks `upstream_failed` — they never run on a bad batch.

Every task also emits real OpenLineage START/COMPLETE/FAIL events via
`lineage.lineage_emitter.emit_lineage` (openlineage-python + FileTransport),
so lineage exists independent of whether the gates pass.

This DAG cannot even be imported on native Windows, let alone executed:
Airflow's core (`airflow.jobs.job`) calls `os.register_at_fork`, a
POSIX-only API, and `airflow.sdk`'s own internal import chain
(`ObjectStoragePath`) fails independently of that. Both are confirmed,
not assumed — see the README for the exact errors. Run this DAG in Google
Colab (the same Linux environment the course's own lab notebooks already
use) or under WSL2/Docker. See README.md "Stage 4 — Orchestration" for the
exact Colab commands.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

MIN_ACCEPTED_DOCUMENTS = 1


@dag(
    dag_id="university_knowledge_hub_pipeline",
    description=(
        "Ingests university documents through Kafka + Pydantic validation, "
        "builds the Bronze/Silver/Gold Delta Lakehouse gated by a real "
        "Great Expectations checkpoint, with OpenLineage events throughout."
    ),
    schedule=None,  # manually triggered / `airflow dags test`, not a cron job
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "ai-university-knowledge-hub", "retries": 0},
    tags=["capstone", "ingestion", "lakehouse", "quality", "lineage"],
)
def university_knowledge_hub_pipeline():
    @task
    def produce_documents() -> dict:
        from lineage.lineage_emitter import emit_lineage
        from producer.document_producer import main as produce

        with emit_lineage("produce_documents"):
            produce()
        return {"status": "produced"}

    @task
    def validate_and_consume(_produced: dict) -> dict:
        from consumer.document_consumer import main as consume
        from lineage.lineage_emitter import emit_lineage

        with emit_lineage("validate_and_consume"):
            return consume()

    @task
    def check_ingestion_quality(consume_result: dict) -> dict:
        from lineage.lineage_emitter import emit_lineage

        with emit_lineage("check_ingestion_quality"):
            accepted = consume_result.get("accepted", 0)
            rejected = consume_result.get("rejected", 0)
            if accepted < MIN_ACCEPTED_DOCUMENTS:
                raise AirflowException(
                    f"Ingestion gate FAILED: only {accepted} document(s) passed "
                    f"validation ({rejected} rejected) — halting the pipeline "
                    "before Bronze/Silver/Gold run on an empty batch."
                )
            return {"gate": "passed", "accepted": accepted, "rejected": rejected}

    @task
    def bronze_load(_gate_result: dict) -> dict:
        from lakehouse.bronze.bronze_loader import main as bronze_main
        from lineage.lineage_emitter import emit_lineage

        with emit_lineage("bronze_load"):
            return bronze_main()

    @task
    def silver_transform(_bronze_result: dict) -> dict:
        from lakehouse.silver.silver_transform import main as silver_main
        from lineage.lineage_emitter import emit_lineage

        with emit_lineage("silver_transform"):
            return silver_main()

    @task
    def quality_checkpoint(_silver_result: dict) -> dict:
        from lineage.lineage_emitter import emit_lineage
        from quality.ge_checkpoint import main as run_quality_checkpoint

        with emit_lineage("quality_checkpoint"):
            passed = run_quality_checkpoint()
            if not passed:
                raise AirflowException(
                    "Great Expectations quality gate FAILED on Silver — "
                    "halting before Gold. See logs/ge_checkpoint.log for "
                    "which expectation(s) failed."
                )
            return {"gate": "passed"}

    @task
    def gold_aggregate(_quality_result: dict) -> dict:
        from lakehouse.gold.gold_aggregate import main as gold_main
        from lineage.lineage_emitter import emit_lineage

        with emit_lineage("gold_aggregate"):
            return gold_main()

    produced = produce_documents()
    consumed = validate_and_consume(produced)
    ingestion_gate = check_ingestion_quality(consumed)
    bronze = bronze_load(ingestion_gate)
    silver = silver_transform(bronze)
    quality_gate = quality_checkpoint(silver)
    gold_aggregate(quality_gate)


university_knowledge_hub_pipeline()
