"""Context construction: turns reranked chunks into a numbered, cited
context block and the strict grounding system prompt.
"""
from __future__ import annotations

SYSTEM_PROMPT = (
    "You are a university knowledge assistant. Answer the student's question "
    "using ONLY the information in the CONTEXT below — never use outside "
    "knowledge, and never guess. Every factual claim in your answer must be "
    "traceable to one of the numbered sources. Cite the source you used with "
    "the format [Source N] after the relevant sentence.\n\n"
    "If the CONTEXT does not contain the answer, respond with EXACTLY this "
    "sentence and nothing else: "
    "\"I don't know based on the available university documents.\""
)


def build_context_block(chunks: list[dict]) -> str:
    return "\n\n".join(
        f"[Source {i + 1}: {c['file_name']}]\n{c['text']}" for i, c in enumerate(chunks)
    )


def build_messages(query: str, chunks: list[dict]) -> list[dict]:
    context = build_context_block(chunks)
    user_content = f"CONTEXT:\n{context}\n\nQUESTION: {query}"
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]


def citations_for(chunks: list[dict]) -> list[str]:
    """Unique source file names, in the order they first appear."""
    seen = []
    for c in chunks:
        if c["file_name"] not in seen:
            seen.append(c["file_name"])
    return seen
