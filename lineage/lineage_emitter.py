"""Real OpenLineage lineage emission — START before a stage runs, COMPLETE
on success, FAIL (with the exception re-raised) on failure.

Uses the real `openlineage-python` client with a local `FileTransport` —
no Marquez server required, same as the Day 4 lab's real-library emitter,
generalized here so every pipeline stage can be wrapped with one line
instead of hand-building each event.
"""
from __future__ import annotations

import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.uuid import generate_new_uuid

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

LINEAGE_LOG_PATH = PROJECT_ROOT / "lineage" / "lineage_events" / "openlineage_run.log"
NAMESPACE = "ai_university_knowledge_hub"
PRODUCER_URI = "https://github.com/SDAIAAcademy/ai-university-knowledge-hub"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _client() -> OpenLineageClient:
    LINEAGE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    transport = FileTransport(FileConfig(log_file_path=str(LINEAGE_LOG_PATH)))
    return OpenLineageClient(transport=transport)


@contextmanager
def emit_lineage(job_name: str):
    """Wraps a pipeline stage with real OpenLineage START/COMPLETE/FAIL events.

    Usage:
        with emit_lineage("bronze_load"):
            bronze_loader.main()
    """
    client = _client()
    run = Run(runId=str(generate_new_uuid()))
    job = Job(namespace=NAMESPACE, name=job_name)

    client.emit(RunEvent(eventType=RunState.START, eventTime=_now(), run=run, job=job, producer=PRODUCER_URI))
    logger.info(f"[LINEAGE] START     {job_name} (run_id={run.runId})")

    try:
        yield
    except Exception:
        client.emit(RunEvent(eventType=RunState.FAIL, eventTime=_now(), run=run, job=job, producer=PRODUCER_URI))
        logger.error(f"[LINEAGE] FAIL      {job_name} (run_id={run.runId})")
        raise
    else:
        client.emit(RunEvent(eventType=RunState.COMPLETE, eventTime=_now(), run=run, job=job, producer=PRODUCER_URI))
        logger.success(f"[LINEAGE] COMPLETE  {job_name} (run_id={run.runId})")
