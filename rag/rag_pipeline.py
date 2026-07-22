"""End-to-end RAG pipeline: chunking -> embeddings/ChromaDB -> BM25 ->
RRF hybrid search -> cross-encoder rerank -> grounded LLM answer + citations.

This is the function the eventual API/UI layer calls for every user
question; run standalone it indexes once and then answers a batch of
example questions, logging full evidence of both the happy path and the
no-hallucination refusal path.
"""
from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from rag.chunking.chunker import build_all_chunks  # noqa: E402
from rag.generation.answer_generator import generate_answer  # noqa: E402
from rag.reranker.cross_encoder_reranker import rerank  # noqa: E402
from rag.retrieval.bm25_index import BM25Index  # noqa: E402
from rag.retrieval.hybrid_search import hybrid_search  # noqa: E402
from rag.vector_db.chroma_store import get_collection, index_chunks  # noqa: E402

HYBRID_TOP_K = 6
RERANK_TOP_K = 3


class RAGPipeline:
    def __init__(self):
        chunks = build_all_chunks()
        self.collection = index_chunks(chunks)
        self.bm25 = BM25Index(chunks)
        logger.info(f"RAG pipeline ready: {len(chunks)} chunks indexed")

    def answer(self, query: str) -> dict:
        hybrid_hits = hybrid_search(self.collection, self.bm25, query, top_k=HYBRID_TOP_K)
        reranked = rerank(query, hybrid_hits, top_k=RERANK_TOP_K)
        result = generate_answer(query, reranked)
        result["query"] = query
        result["hybrid_candidate_count"] = len(hybrid_hits)
        return result


EXAMPLE_QUESTIONS = [
    "What are the graduation requirements?",
    "What courses are required for Computer Science?",
    "How many credit hours are needed to graduate?",
    "What is the deadline for course withdrawal?",
    "What is the attendance policy?",
    "What scholarships are available?",
    # Deliberately out of scope — no document covers this — proves the
    # pipeline refuses instead of hallucinating.
    "What is the university's parking permit policy?",
]


def main() -> None:
    logger.add(
        PROJECT_ROOT / "logs" / "rag_pipeline.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="1 MB",
    )

    pipeline = RAGPipeline()

    for query in EXAMPLE_QUESTIONS:
        result = pipeline.answer(query)
        logger.info("=" * 70)
        logger.info(f"Q: {query}")
        logger.info(f"A: {result['answer']}")
        logger.info(f"Citations: {result['citations']}")
        logger.info(
            f"(hybrid candidates: {result['hybrid_candidate_count']}, "
            f"chunks used: {len(result['context_used'])})"
        )


if __name__ == "__main__":
    main()
