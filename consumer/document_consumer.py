"""Kafka consumer for the AI University Knowledge Hub ingestion stage.

Subscribes to `university.documents.raw` and validates every message against
`DocumentIngestContract` — this is the schema-validation boundary the rubric
requires. Valid events are forwarded to `university.documents.validated`
(the hook the Bronze Lakehouse stage consumes next) and appended to a local
manifest. Invalid events are published to `university.documents.dlq` AND
written to `quarantine_zone/` with the exact rejection reason recorded,
never silently dropped.
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer
from loguru import logger
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from schemas.document_contract import DocumentIngestContract  # noqa: E402

TOPIC_RAW = "university.documents.raw"
TOPIC_VALIDATED = "university.documents.validated"
TOPIC_DLQ = "university.documents.dlq"
BOOTSTRAP_SERVERS = "localhost:9092"
CONSUMER_GROUP = "document-ingestion-validator"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
QUARANTINE_DIR = PROJECT_ROOT / "quarantine_zone"
MANIFEST_PATH = PROJECT_ROOT / "data" / "bronze_manifest" / "validated_events.jsonl"


def route_valid(producer: KafkaProducer, event: DocumentIngestContract) -> None:
    payload = json.loads(event.model_dump_json())
    producer.send(TOPIC_VALIDATED, key=str(event.document_id), value=payload)

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")

    logger.success(
        f"ACCEPTED  {event.file_name!r} (id={event.document_id}, "
        f"type={event.file_type}, category={event.category}) -> {TOPIC_VALIDATED}"
    )


def route_invalid(producer: KafkaProducer, raw_value: dict, reason: str) -> None:
    rejected_at = datetime.now(timezone.utc).isoformat()
    dlq_payload = {
        "original_event": raw_value,
        "rejection_reason": reason,
        "rejected_at": rejected_at,
    }
    producer.send(TOPIC_DLQ, value=dlq_payload)

    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    file_name = raw_value.get("file_name", "unknown")
    doc_id = raw_value.get("document_id", "no-id")
    safe_name = f"{doc_id}_{Path(str(file_name)).stem}.json".replace(" ", "_")
    with open(QUARANTINE_DIR / safe_name, "w", encoding="utf-8") as f:
        json.dump(dlq_payload, f, indent=2)

    logger.error(
        f"REJECTED  {file_name!r} (id={doc_id}) -> {TOPIC_DLQ} | reason: {reason}"
    )


def main(timeout_s: int = 10) -> dict:
    logger.add(
        PROJECT_ROOT / "logs" / "consumer.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="1 MB",
    )

    consumer = KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout_s * 1000,
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )

    accepted, rejected = 0, 0
    rejection_reasons: Counter[str] = Counter()

    logger.info(f"Consuming '{TOPIC_RAW}' as group '{CONSUMER_GROUP}'...")
    for record in consumer:
        raw_value = record.value
        try:
            event = DocumentIngestContract.model_validate(raw_value)
            route_valid(producer, event)
            accepted += 1
        except ValidationError as exc:
            reason = exc.errors()[0]["msg"]
            route_invalid(producer, raw_value, reason)
            rejection_reasons[reason.split(" (")[0].split(" —")[0]] += 1
            rejected += 1

    producer.flush()
    producer.close()
    consumer.close()

    total = accepted + rejected
    logger.info("=" * 60)
    logger.info("INGESTION VALIDATION SUMMARY")
    logger.info(f"  Total messages consumed : {total}")
    logger.info(f"  Accepted (Bronze-bound) : {accepted}")
    logger.info(f"  Rejected (DLQ+quarantine): {rejected}")
    if rejection_reasons:
        logger.info("  Rejection reason breakdown:")
        for reason, count in rejection_reasons.most_common():
            logger.info(f"    [{count}x] {reason}")
    logger.info("=" * 60)

    return {
        "total": total,
        "accepted": accepted,
        "rejected": rejected,
        "rejection_reasons": dict(rejection_reasons),
    }


if __name__ == "__main__":
    main()
