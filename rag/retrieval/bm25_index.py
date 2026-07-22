"""BM25 keyword index — finds exact term matches (course codes, policy
numbers, acronyms) that dense semantic search often misses.
"""
from __future__ import annotations

import sys
from pathlib import Path

from rank_bm25 import BM25Okapi

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class BM25Index:
    def __init__(self, chunks: list[dict]):
        self.chunks = chunks
        tokenized = [c["text"].lower().split() for c in chunks]
        self.bm25 = BM25Okapi(tokenized)

    def search(self, query: str, top_k: int = 10) -> list[dict]:
        scores = self.bm25.get_scores(query.lower().split())
        ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)
        return [self.chunks[idx] for idx, _score in ranked[:top_k]]


if __name__ == "__main__":
    from rag.chunking.chunker import build_all_chunks

    chunks = build_all_chunks()
    index = BM25Index(chunks)
    for hit in index.search("withdrawal deadline week", top_k=3):
        print(f"  [{hit['chunk_id']}] {hit['text'][:100]}...")
