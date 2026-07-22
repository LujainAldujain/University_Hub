"""Chunking: reads governed documents from Silver and splits them into
overlapping, sentence-aligned chunks for embedding and retrieval.

Reads via the `deltalake` package (pure Rust/Python Delta reader) rather
than spinning up a SparkSession — RAG only needs a handful of small text
rows, not distributed processing.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

from deltalake import DeltaTable

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

SILVER_TABLE_PATH = str(PROJECT_ROOT / "lakehouse" / "silver" / "documents_silver")

SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def read_silver_documents() -> list[dict]:
    """Reads valid, governed documents from the Silver Delta table."""
    dt = DeltaTable(SILVER_TABLE_PATH)
    rows = dt.to_pyarrow_table().to_pylist()
    return [r for r in rows if r["is_valid"] and r["clean_text"]]


def chunk_document(
    doc: dict, sentences_per_chunk: int = 4, overlap_sentences: int = 1
) -> list[dict]:
    """Splits one document's clean_text into overlapping sentence-group chunks.

    Each chunk carries the metadata needed for a citation later: file_name,
    category, and document_id.
    """
    sentences = [s for s in SENTENCE_SPLIT_RE.split(doc["clean_text"].strip()) if s.strip()]
    stride = max(1, sentences_per_chunk - overlap_sentences)

    chunks = []
    for i in range(0, len(sentences), stride):
        group = sentences[i : i + sentences_per_chunk]
        if not group:
            continue
        chunk_text = " ".join(group)
        chunks.append(
            {
                "chunk_id": f"{doc['file_name']}_chunk_{i:03d}",
                "text": chunk_text,
                "file_name": doc["file_name"],
                "category": doc["category"],
                "document_id": doc["document_id"],
            }
        )
        if i + sentences_per_chunk >= len(sentences):
            break
    return chunks


def build_all_chunks() -> list[dict]:
    documents = read_silver_documents()
    all_chunks = []
    for doc in documents:
        all_chunks.extend(chunk_document(doc))
    return all_chunks


if __name__ == "__main__":
    chunks = build_all_chunks()
    print(f"{len(chunks)} chunks built from Silver documents")
    for c in chunks[:3]:
        print(f"  [{c['chunk_id']}] ({c['category']}) {c['text'][:100]}...")
