"""Embeds chunks with a sentence-transformer bi-encoder and indexes them in
a persistent ChromaDB collection (HNSW under the hood — the same index
type used by Pinecone/Weaviate).
"""
from __future__ import annotations

import sys
from pathlib import Path

import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

CHROMA_PERSIST_DIR = str(PROJECT_ROOT / "rag" / "vector_db" / "chroma_persistent")
COLLECTION_NAME = "university_knowledge_base"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"


def get_collection():
    """Returns the persistent ChromaDB collection, creating it if needed."""
    client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
    ef = SentenceTransformerEmbeddingFunction(model_name=EMBEDDING_MODEL)
    return client.get_or_create_collection(COLLECTION_NAME, embedding_function=ef)


def index_chunks(chunks: list[dict]) -> "chromadb.Collection":
    """Upserts all chunks into the vector store (idempotent — safe to re-run)."""
    collection = get_collection()
    collection.upsert(
        ids=[c["chunk_id"] for c in chunks],
        documents=[c["text"] for c in chunks],
        metadatas=[
            {
                "file_name": c["file_name"],
                "category": c["category"],
                "document_id": c["document_id"],
            }
            for c in chunks
        ],
    )
    return collection


def vector_search(collection, query: str, n_results: int = 6) -> list[dict]:
    """Dense semantic search — returns candidates in the shape hybrid_search expects."""
    results = collection.query(query_texts=[query], n_results=n_results)
    hits = []
    for chunk_id, text, meta in zip(
        results["ids"][0], results["documents"][0], results["metadatas"][0]
    ):
        hits.append({"chunk_id": chunk_id, "text": text, **meta})
    return hits


if __name__ == "__main__":
    from rag.chunking.chunker import build_all_chunks

    chunks = build_all_chunks()
    collection = index_chunks(chunks)
    print(f"Indexed {collection.count()} chunks into ChromaDB (HNSW) at {CHROMA_PERSIST_DIR}")

    print("\nSmoke-test query: 'What is the attendance policy?'")
    for h in vector_search(collection, "What is the attendance policy?", n_results=3):
        print(f"  [{h['chunk_id']}] {h['text'][:100]}...")
