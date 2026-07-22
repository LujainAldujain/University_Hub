"""Cross-encoder reranking — the Stage-2 precision gate.

The bi-encoder (ChromaDB embeddings) retrieves candidates fast by embedding
query and document independently. A cross-encoder scores each
(query, document) pair jointly instead — far more accurate because it can
model the interaction between query and document tokens directly, at the
cost of one inference call per candidate (fine for reranking ~5-10
candidates, not for scanning millions of documents).
"""
from __future__ import annotations

CROSS_ENCODER_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"

_model = None


def _get_model():
    global _model
    if _model is None:
        from sentence_transformers import CrossEncoder

        _model = CrossEncoder(CROSS_ENCODER_MODEL)
    return _model


def rerank(query: str, candidates: list[dict], top_k: int = 3) -> list[dict]:
    if not candidates:
        return []
    model = _get_model()
    pairs = [(query, c["text"]) for c in candidates]
    scores = model.predict(pairs)
    ranked = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
    return [{**chunk, "rerank_score": float(score)} for score, chunk in ranked[:top_k]]


if __name__ == "__main__":
    import sys
    from pathlib import Path

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(PROJECT_ROOT))

    from rag.chunking.chunker import build_all_chunks
    from rag.retrieval.bm25_index import BM25Index
    from rag.retrieval.hybrid_search import hybrid_search
    from rag.vector_db.chroma_store import get_collection

    chunks = build_all_chunks()
    collection = get_collection()
    bm25 = BM25Index(chunks)

    query = "What GPA do I need to keep my scholarship?"
    hybrid_hits = hybrid_search(collection, bm25, query, top_k=6)
    print(f"Query: {query!r}\nHybrid candidates: {len(hybrid_hits)}")

    reranked = rerank(query, hybrid_hits, top_k=3)
    print("\nAfter cross-encoder reranking:")
    for hit in reranked:
        print(f"  [{hit['rerank_score']:.3f}] [{hit['chunk_id']}] {hit['text'][:100]}...")
