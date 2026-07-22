"""Machine-enforceable schema for a university document entering the ingestion pipeline.

Any payload that fails this contract is rejected at the consumer boundary and
routed to the dead-letter queue / quarantine zone before it can reach Bronze.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

DocumentType = Literal["pdf", "docx", "txt"]
DocumentCategory = Literal[
    "policy",
    "course_catalog",
    "handbook",
    "scholarship",
    "general",
]

MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB
ALLOWED_EXTENSIONS: dict[str, DocumentType] = {
    ".pdf": "pdf",
    ".docx": "docx",
    ".txt": "txt",
}


class DocumentIngestContract(BaseModel):
    """Data contract enforced at the Kafka consumer boundary.

    strict=False because Kafka delivers JSON-decoded primitives (str/int/float)
    over the wire; we still forbid unknown fields so producers can't silently
    smuggle extra, unvalidated data through the gate.
    """

    model_config = ConfigDict(strict=False, extra="forbid")

    document_id: UUID = Field(default_factory=uuid4)
    file_name: str
    file_path: str
    file_type: DocumentType
    file_size_bytes: int
    category: DocumentCategory = "general"
    course_code: str | None = None
    uploaded_by: str
    upload_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("file_name")
    @classmethod
    def file_name_not_blank(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("file_name is required — cannot be blank")
        if "/" in v or "\\" in v or ".." in v:
            raise ValueError(f"file_name '{v}' must be a bare filename, not a path")
        return v

    @field_validator("uploaded_by")
    @classmethod
    def uploaded_by_not_blank(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("uploaded_by is required — cannot be blank")
        return v

    @field_validator("file_size_bytes")
    @classmethod
    def size_in_bounds(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"file_size_bytes must be > 0 (got {v} — empty upload)")
        if v > MAX_FILE_SIZE_BYTES:
            raise ValueError(
                f"file_size_bytes {v} exceeds max allowed {MAX_FILE_SIZE_BYTES} "
                "(50 MB) — reject oversized upload"
            )
        return v

    @model_validator(mode="after")
    def extension_matches_declared_type(self) -> "DocumentIngestContract":
        ext = Path(self.file_name).suffix.lower()
        expected_type = ALLOWED_EXTENSIONS.get(ext)
        if expected_type is None:
            raise ValueError(
                f"file_name '{self.file_name}' has unsupported extension '{ext}' "
                f"— only {sorted(ALLOWED_EXTENSIONS)} are accepted"
            )
        if expected_type != self.file_type:
            raise ValueError(
                f"declared file_type '{self.file_type}' does not match the actual "
                f"extension '{ext}' (expected '{expected_type}')"
            )
        return self

    @model_validator(mode="after")
    def file_exists_on_disk(self) -> "DocumentIngestContract":
        if not os.path.isfile(self.file_path):
            raise ValueError(f"file_path '{self.file_path}' does not exist on disk")
        return self
