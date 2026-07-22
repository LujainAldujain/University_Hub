"""Kafka producer for the AI University Knowledge Hub ingestion stage.

Scans the `incoming_documents/` landing folder (simulating a document upload
endpoint) and publishes one raw JSON event per file to the
`university.documents.raw` topic. The producer intentionally does NOT
validate — schema validation is the consumer's job, at the ingestion
boundary (`schemas.document_contract.DocumentIngestContract`). This mirrors
real upstream clients, which cannot be trusted to send well-formed data.

A few synthetic malformed events (not backed by any real file) are appended
to exercise validator branches that the sample fixtures alone wouldn't
reach, so both the happy path and every rejection path are demonstrated.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from loguru import logger

TOPIC_RAW = "university.documents.raw"
BOOTSTRAP_SERVERS = "localhost:9092"

INCOMING_DIR = Path(__file__).resolve().parent.parent / "incoming_documents"

CATEGORY_KEYWORDS = {
    "graduation": "policy",
    "withdrawal": "policy",
    "attendance": "policy",
    "scholarship": "scholarship",
    "cs_program": "course_catalog",
}

UPLOADERS = {
    "policy": "registrar_office_bot",
    "scholarship": "financial_aid_office_bot",
    "course_catalog": "cs_department_admin",
    "general": "campus_portal_uploader",
}


def infer_category(file_stem: str) -> str:
    for keyword, category in CATEGORY_KEYWORDS.items():
        if keyword in file_stem.lower():
            return category
    return "general"


def build_event_for_file(path: Path) -> dict:
    """Builds a RAW (unvalidated) ingestion event exactly as an upstream
    client would send it — including whatever extension/type it claims,
    right or wrong.
    """
    ext = path.suffix.lower().lstrip(".")
    category = infer_category(path.stem)
    return {
        "document_id": str(uuid.uuid4()),
        "file_name": path.name,
        "file_path": str(path.resolve()),
        "file_type": ext,  # not normalized/validated — producer just forwards it
        "file_size_bytes": path.stat().st_size,
        "category": category,
        "uploaded_by": UPLOADERS.get(category, "campus_portal_uploader"),
        "upload_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def build_synthetic_bad_events() -> list[dict]:
    """Malformed events not backed by any real file, to exercise validator
    branches the fixture files alone don't reach: a type/extension mismatch,
    a missing required field, and a reference to a file that doesn't exist.
    """
    now = datetime.now(timezone.utc).isoformat()
    return [
        {
            # extension says .txt but the client claims file_type "pdf"
            "document_id": str(uuid.uuid4()),
            "file_name": "mislabeled_policy.txt",
            "file_path": str((INCOMING_DIR / "graduation_requirements.txt").resolve()),
            "file_type": "pdf",
            "file_size_bytes": 1234,
            "category": "policy",
            "uploaded_by": "buggy_mobile_app_v1",
            "upload_timestamp": now,
        },
        {
            # required field "uploaded_by" missing entirely
            "document_id": str(uuid.uuid4()),
            "file_name": "anonymous_upload.txt",
            "file_path": str((INCOMING_DIR / "scholarships_financial_aid.txt").resolve()),
            "file_type": "txt",
            "file_size_bytes": 500,
            "category": "general",
            "upload_timestamp": now,
        },
        {
            # file_path points at a file that was never actually written to disk
            "document_id": str(uuid.uuid4()),
            "file_name": "ghost_document.txt",
            "file_path": str((INCOMING_DIR / "does_not_exist_on_disk.txt").resolve()),
            "file_type": "txt",
            "file_size_bytes": 42,
            "category": "general",
            "uploaded_by": "campus_portal_uploader",
            "upload_timestamp": now,
        },
    ]


def main() -> None:
    logger.add(
        Path(__file__).resolve().parent.parent / "logs" / "producer.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="1 MB",
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    files = sorted(p for p in INCOMING_DIR.iterdir() if p.is_file())
    events = [build_event_for_file(p) for p in files] + build_synthetic_bad_events()

    logger.info(f"Publishing {len(events)} document events to '{TOPIC_RAW}'")
    for event in events:
        producer.send(TOPIC_RAW, key=event["document_id"], value=event)
        logger.info(
            f"  -> sent {event['file_name']} "
            f"(type={event['file_type']!r}, size={event['file_size_bytes']}, "
            f"category={event.get('category')!r})"
        )

    producer.flush()
    producer.close()
    logger.success(f"Done. {len(events)} events published to Kafka topic '{TOPIC_RAW}'.")


if __name__ == "__main__":
    main()
