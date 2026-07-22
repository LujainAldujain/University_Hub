"""Hybrid search: fuses dense vector (ChromaDB/HNSW) and sparse BM25
keyword results with Reciprocal Rank Fusion — parameter-free, and
consistently outperforms a hand-tuned weighted linear combination.
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def reciprocal_rank_fusion(
    vector_hits: list[dict], bm25_hits: list[dict], k: int = 60, top_k: int = 6
) -> list[dict]:
    """RRF score = sum(1 / (k + rank)) across both ranked result lists."""
    rrf_scores: dict[str, float] = {}
    id_to_chunk: dict[str, dict] = {}

    for rank, hit in enumerate(vector_hits):
        cid = hit["chunk_id"]
        rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (k + rank + 1)
        id_to_chunk[cid] = hit

    for rank, hit in enumerate(bm25_hits):
        cid = hit["chunk_id"]
        rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (k + rank + 1)
        id_to_chunk[cid] = hit

    sorted_ids = sorted(rrf_scores, key=lambda cid: rrf_scores[cid], reverse=True)
    return [id_to_chunk[cid] for cid in sorted_ids[:top_k]]


def hybrid_search(collection, bm25_index, query: str, top_k: int = 6) -> list[dict]:
    from rag.vector_db.chroma_store import vector_search

    vector_hits = vector_search(collection, query, n_results=top_k)
    bm25_hits = bm25_index.search(query, top_k=top_k)
    return reciprocal_rank_fusion(vector_hits, bm25_hits, top_k=top_k)


if __name__ == "__main__":
    from rag.chunking.chunker import build_all_chunks
    from rag.retrieval.bm25_index import BM25Index
    from rag.vector_db.chroma_store import get_collection

    chunks = build_all_chunks()
    collection = get_collection()
    bm25 = BM25Index(chunks)

    query = "What is the deadline to withdraw from a course?"
    print(f"Query: {query!r}")
    for hit in hybrid_search(collection, bm25, query, top_k=5):
        print(f"  [{hit['chunk_id']}] {hit['text'][:100]}...")
