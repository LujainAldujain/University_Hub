# Capstone Rubric Mapping

100 points total, pass mark 60. This maps every line item in the capstone
rubric to the exact file(s) that satisfy it and where its evidence lives.

## 1. Capstone Rubric (100 pts)

### Ingestion — 20 pts

| Requirement | Where |
|---|---|
| Kafka producer | [`producer/document_producer.py`](../producer/document_producer.py) |
| Kafka consumer | [`consumer/document_consumer.py`](../consumer/document_consumer.py) |
| Pydantic data contract at the ingestion boundary | [`schemas/document_contract.py`](../schemas/document_contract.py) |
| Malformed records → quarantine/DLQ + rejection reason recorded | `consumer/document_consumer.py` → `university.documents.dlq` topic + `quarantine_zone/*.json` |

**Evidence**: [`logs/producer.log`](../logs/producer.log), [`logs/consumer.log`](../logs/consumer.log) — 10 events published, 5 accepted / 5 rejected across 5 distinct failure modes, cross-checked against the real Kafka broker's topic offsets (raw=10, validated=5, dlq=5).

### Delta Lakehouse — 25 pts

| Requirement | Where |
|---|---|
| Bronze/Silver/Gold in Delta Lake | [`lakehouse/bronze/bronze_loader.py`](../lakehouse/bronze/bronze_loader.py), [`lakehouse/silver/silver_transform.py`](../lakehouse/silver/silver_transform.py), [`lakehouse/gold/gold_aggregate.py`](../lakehouse/gold/gold_aggregate.py) |
| Real `MERGE` (upsert) keyed on a business key | `silver_transform.py` — `MERGE ... ON target.file_name = source.file_name` |
| Demonstrated schema enforcement | `silver_transform.py::demonstrate_schema_enforcement` |
| Gold is a genuine aggregate, not a copy of Silver | `gold_aggregate.py` — `groupBy("category")` rollup |

**Evidence**: [`logs/bronze_loader.log`](../logs/bronze_loader.log), [`logs/silver_transform.log`](../logs/silver_transform.log), [`logs/gold_aggregate.log`](../logs/gold_aggregate.log) — a simulated document revision proves MERGE updates in place (5 rows before/after, `document_id` changed); a live schema-mismatch rejection (`_LEGACY_ERROR_TEMP_DELTA_0007`); Gold's 5-document-row → 3-category-row rollup with a different schema.

### RAG Pipeline — 25 pts

| Requirement | Where |
|---|---|
| Document loader | [`utils/document_loader.py`](../utils/document_loader.py) (txt/docx/pdf) |
| Chunking | [`rag/chunking/chunker.py`](../rag/chunking/chunker.py) |
| Embedding generation | [`rag/vector_db/chroma_store.py`](../rag/vector_db/chroma_store.py) (all-MiniLM-L6-v2) |
| Vector database / semantic search | `chroma_store.py` (ChromaDB, HNSW) |
| BM25 retrieval | [`rag/retrieval/bm25_index.py`](../rag/retrieval/bm25_index.py) |
| Hybrid search + Reciprocal Rank Fusion | [`rag/retrieval/hybrid_search.py`](../rag/retrieval/hybrid_search.py) |
| Cross-encoder reranking | [`rag/reranker/cross_encoder_reranker.py`](../rag/reranker/cross_encoder_reranker.py) (ms-marco-MiniLM-L-6-v2) |
| Context construction + citations | [`rag/generation/prompt_builder.py`](../rag/generation/prompt_builder.py) |
| Grounded LLM responses, no hallucination | [`rag/generation/answer_generator.py`](../rag/generation/answer_generator.py) (Qwen2.5-1.5B-Instruct, local) |

**Evidence**: [`logs/rag_pipeline.log`](../logs/rag_pipeline.log) — all 6 in-scope example questions answered correctly with the right citation; a 7th, deliberately out-of-scope question (parking policy) correctly refused with the exact "I don't know" sentence and zero citations, even though hybrid search still returned candidates.

### Orchestration — 15 pts

| Requirement | Where |
|---|---|
| Real Airflow DAG, correct dependencies | [`airflow/dags/university_pipeline_dag.py`](../airflow/dags/university_pipeline_dag.py) |
| Failed quality gate halts the pipeline | `check_ingestion_quality` and `quality_checkpoint` tasks — every downstream task depends on its gate via TaskFlow argument-passing, `all_success` trigger rule |

**Evidence**: [`logs/airflow_dag_test.log`](../logs/airflow_dag_test.log) — executed with `airflow dags test university_knowledge_hub_pipeline 2026-01-01` in Google Colab (confirmed: `os.register_at_fork` is missing on native Windows, POSIX-only, so this genuinely requires a Linux runtime). Airflow's own log confirms `DagRun Finished: ... state=success`; all 7 tasks ran in the correct order with the same real results as the local runs (5/10 documents accepted, MERGE + schema-enforcement rejection on Silver, all 6 GX expectations passed, 5→3 row Gold rollup).

### Quality Gate + Lineage — 15 pts

| Requirement | Where |
|---|---|
| Great Expectations checks that gate the pipeline | [`quality/ge_checkpoint.py`](../quality/ge_checkpoint.py) |
| OpenLineage START/COMPLETE/FAIL per stage | [`lineage/lineage_emitter.py`](../lineage/lineage_emitter.py) |

**Evidence**: [`logs/pipeline_with_lineage.log`](../logs/pipeline_with_lineage.log) — happy path, all 7 stages START→COMPLETE, all 6 GX expectations PASSED. [`logs/quality_gate_failure_demo.log`](../logs/quality_gate_failure_demo.log) — [`scripts/demo_quality_gate_failure.py`](../scripts/demo_quality_gate_failure.py) injects a duplicate `file_name` into Silver, proves (via a code assertion, not just a log line) that `gold_aggregate` is never called while a real OpenLineage FAIL event fires, then cleans up the injected row.

## 2. GitHub & Documentation Requirements

- **2.1 Mandatory**: repository is on GitHub, incremental commit history (see `git log`), continuously updated.
- **2.2 Every repo must include**: this file + [README.md](../README.md) (project description, setup/run/expected output, architecture, training-program attribution, link to [github.com/SDAIAAcademy](https://github.com/SDAIAAcademy)), a `.gitignore` excluding generated data and local-only tooling, meaningful incremental commits.
- **2.3 Encouraged**: supporting the Saudi tech community on GitHub (starring, following, contributing) — not scored, left to the student's own GitHub activity.
