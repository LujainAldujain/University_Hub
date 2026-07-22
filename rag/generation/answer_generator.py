"""Grounded answer generation using a small local, open-source instruct
model (no API key required) — Qwen2.5-1.5B-Instruct via Hugging Face
transformers, run on CPU.

The LLM never sees anything beyond the retrieved+reranked chunks — the
strict system prompt (rag/generation/prompt_builder.py) instructs it to
answer only from that context and to say it doesn't know otherwise, which
is what actually prevents hallucination here; the model choice is
secondary to that constraint.
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from rag.generation.prompt_builder import build_messages, citations_for  # noqa: E402

GENERATION_MODEL = "Qwen/Qwen2.5-1.5B-Instruct"
REFUSAL_TEXT = "I don't know based on the available university documents."

_tokenizer = None
_model = None


def _get_model():
    global _tokenizer, _model
    if _model is None:
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        _tokenizer = AutoTokenizer.from_pretrained(GENERATION_MODEL)
        _model = AutoModelForCausalLM.from_pretrained(GENERATION_MODEL, dtype=torch.float32)
    return _tokenizer, _model


def generate_answer(query: str, reranked_chunks: list[dict], max_new_tokens: int = 200) -> dict:
    """Returns {"answer": str, "citations": list[str], "context_used": list[dict]}.

    If reranked_chunks is empty (nothing survived retrieval), refuses
    immediately without ever calling the LLM — there is nothing to ground on.
    """
    if not reranked_chunks:
        return {"answer": REFUSAL_TEXT, "citations": [], "context_used": []}

    tokenizer, model = _get_model()
    messages = build_messages(query, reranked_chunks)

    prompt = tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )
    inputs = tokenizer(prompt, return_tensors="pt")

    import torch

    with torch.no_grad():
        output_ids = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            do_sample=False,
            temperature=None,
            top_p=None,
            top_k=None,
            pad_token_id=tokenizer.eos_token_id,
        )
    generated = output_ids[0][inputs["input_ids"].shape[1] :]
    answer = tokenizer.decode(generated, skip_special_tokens=True).strip()

    is_refusal = REFUSAL_TEXT.lower() in answer.lower()
    return {
        "answer": answer,
        "citations": [] if is_refusal else citations_for(reranked_chunks),
        "context_used": reranked_chunks,
    }


if __name__ == "__main__":
    from rag.chunking.chunker import build_all_chunks
    from rag.reranker.cross_encoder_reranker import rerank
    from rag.retrieval.bm25_index import BM25Index
    from rag.retrieval.hybrid_search import hybrid_search
    from rag.vector_db.chroma_store import get_collection

    chunks = build_all_chunks()
    collection = get_collection()
    bm25 = BM25Index(chunks)

    query = "What is the attendance policy?"
    hybrid_hits = hybrid_search(collection, bm25, query, top_k=6)
    reranked = rerank(query, hybrid_hits, top_k=3)

    result = generate_answer(query, reranked)
    print(f"Q: {query}")
    print(f"A: {result['answer']}")
    print(f"Citations: {result['citations']}")
