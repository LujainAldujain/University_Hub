# AI University Knowledge Hub

An AI-powered university knowledge platform: students ask natural-language
questions about official university documents (graduation requirements,
course catalogs, attendance policy, withdrawal deadlines, scholarships, ...)
and get answers grounded strictly in retrieved documents, with citations —
never hallucinated.

Built for the **Modern Data Engineering for AI Systems** capstone (SDAIA
Academy). See `docs/CAPSTONE_RUBRIC_MAPPING.md` for how each rubric line item
is satisfied.

## Architecture

```
Kafka (producer/consumer + Pydantic contract)
        │  valid documents
        ▼
Delta Lakehouse (Bronze → Silver → Gold, real MERGE)
        │  cleaned + aggregated knowledge
        ▼
RAG pipeline (chunking → embeddings → hybrid search → rerank → grounded LLM answer)
        ▲
Airflow DAG orchestrates every stage above; Great Expectations gates it;
OpenLineage emits START/COMPLETE/FAIL events per stage.
```

## Stage 1 — Ingestion (done)

Real Apache Kafka (4.2.1, KRaft mode, no Zookeeper/Docker) + a strict
Pydantic v2 data contract at the consumer boundary.

- `producer/document_producer.py` — scans `incoming_documents/` and
  publishes one raw JSON event per file to `university.documents.raw`.
  Deliberately does not validate — that's the consumer's job.
- `schemas/document_contract.py` — `DocumentIngestContract`: enforces
  supported file type, extension/type match, non-zero and bounded file
  size, and file existence on disk.
- `consumer/document_consumer.py` — validates every message. Valid events
  go to `university.documents.validated` + `data/bronze_manifest/` (the
  hook the Lakehouse Bronze stage will read next). Invalid events go to
  `university.documents.dlq` **and** `quarantine_zone/*.json` with the
  exact rejection reason recorded.

### Running it

```bash
# one-time setup
uv venv --python 3.11
uv pip install -r requirements.txt

# start Kafka (see kafka_2.13-4.2.1/, KRaft standalone mode) then:
.venv/Scripts/python.exe scripts/seed_sample_documents.py   # generates fixtures
.venv/Scripts/python.exe producer/document_producer.py
.venv/Scripts/python.exe consumer/document_consumer.py
```

### Proven evidence (see `logs/producer.log`, `logs/consumer.log`)

10 raw events published → **5 accepted** (Bronze-bound) / **5 rejected**
(DLQ + quarantined), one of each distinct failure mode:

| Rejected file | Reason |
|---|---|
| `empty_handbook.txt` | zero-byte upload (`file_size_bytes` must be > 0) |
| `corrupted_upload.exe` | disallowed file type |
| `mislabeled_policy.txt` | declared `file_type` doesn't match actual extension |
| `anonymous_upload.txt` | required field `uploaded_by` missing |
| `ghost_document.txt` | referenced `file_path` doesn't exist on disk |

Verified directly against the Kafka broker (not just local logs):
`university.documents.raw` = 10 messages, `.validated` = 5, `.dlq` = 5.

### Windows-specific note

Kafka's `--delete` topic operation is unreliable on native Windows (the JVM
keeps memory-mapped log segments locked, which can crash the broker with a
`KafkaStorageException`). Avoid deleting topics in this local dev setup —
if you need a clean slate, stop the broker, delete `kafka_2.13-4.2.1/kraft-logs/`,
and re-run `kafka-storage.sh format`. This is a Windows JVM file-locking
limitation, not a pipeline bug; production deployments should run on
Linux/containers as usual.

## Stage 2 — Delta Lakehouse (done)

Real PySpark 3.5.0 + delta-spark 3.2.0. Bronze → Silver → Gold, a real
`MERGE` upsert on a business key, and a live schema-enforcement rejection.

- `lakehouse/bronze/bronze_loader.py` — reads Stage 1's validated manifest,
  extracts raw text per document (`utils/document_loader.py`: txt/docx/pdf),
  lands it as-is into a Delta table (schema-enforced, rebuilt from the
  manifest each run).
- `lakehouse/silver/silver_transform.py` — cleans text (unicode
  normalization, whitespace collapse), computes `word_count`, and **MERGEs**
  into Silver keyed on **`file_name`** — the true business key (a document's
  canonical identity across revisions), *not* `document_id` (which is a new
  UUID every re-upload). Also simulates a real revision — a corrected
  `graduation_requirements.txt` re-uploaded with a new `document_id` — to
  prove the MERGE updates the existing row rather than duplicating it, and
  demonstrates Delta rejecting a write with an undeclared column.
- `lakehouse/gold/gold_aggregate.py` — a genuine aggregate over Silver
  (per-category document count, word totals, distinct uploaders, last
  update), **not** a copy of Silver, with `OPTIMIZE` applied.

### Lakehouse setup (Windows-specific)

PySpark 3.5 needs three things this project provisions locally rather than
relying on whatever's already on the machine:

```bash
# 1. A JDK 17 side-by-side with the system Java — newer JDKs break Hadoop's
#    UserGroupInformation (Subject.getSubject was removed upstream)
curl -L -o jdk17.zip "https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jdk/hotspot/normal/eclipse"
unzip jdk17.zip -d jdk17_extracted && mv jdk17_extracted/jdk-* ./jdk17

# 2. winutils.exe + hadoop.dll — Spark's Hadoop client shells out to these
#    on Windows even for purely local file operations
mkdir -p hadoop_home/bin
curl -L -o hadoop_home/bin/winutils.exe "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe"
curl -L -o hadoop_home/bin/hadoop.dll "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll"

# 3. pyspark + delta-spark
uv pip install pyspark==3.5.0 delta-spark==3.2.0
```

`configs/spark_session.py` sets `JAVA_HOME`/`HADOOP_HOME`/`PYSPARK_PYTHON`
from these local paths automatically — no manual env var exports needed for
every run, and the system's default Java/Python are never touched.

### Running it

```bash
.venv/Scripts/python.exe lakehouse/bronze/bronze_loader.py
.venv/Scripts/python.exe lakehouse/silver/silver_transform.py
.venv/Scripts/python.exe lakehouse/gold/gold_aggregate.py
```

### Proven evidence (see `logs/{bronze_loader,silver_transform,gold_aggregate}.log`)

- Bronze: 5 rows written, real Delta table with `_delta_log`.
- Silver MERGE: `graduation_requirements.txt` — 5 rows before, 5 rows after
  a 1-row revision upsert; `document_id` changed (`bb2ad677...` →
  `6d618cc7...`), row count unchanged — confirms **update, not duplicate**.
- Schema enforcement: an append with an injected `injected_malicious_field`
  column was rejected — `[_LEGACY_ERROR_TEMP_DELTA_0007] A schema mismatch
  detected when writing to the Delta table`.
- Gold: 5 Silver document rows → 3 category rows (`policy`, `course_catalog`,
  `scholarship`), a different schema (rollup columns) — a genuine aggregate.

## Stage 4 — Orchestration (code complete — execute in Colab, not locally)

`airflow/dags/university_pipeline_dag.py` wires every stage above into a
real Apache Airflow DAG (TaskFlow API):

```
produce_documents -> validate_and_consume -> check_ingestion_quality
    -> bronze_load -> silver_transform -> gold_aggregate
```

`check_ingestion_quality` is the interim quality gate: it reads the real
accept/reject counts the consumer returns (pushed to XCom automatically)
and raises `AirflowException` if zero documents passed validation. Every
downstream task depends on it through TaskFlow's argument-passing, which
Airflow turns into real task edges with the default `all_success` trigger
rule — so a failed gate leaves Bronze/Silver/Gold `upstream_failed`, never
executed on an empty batch. Once Stage 5 (Great Expectations/OpenLineage)
is built, this gate gets replaced/augmented with a real GX checkpoint task
and START/COMPLETE/FAIL lineage emission around each stage.

### Why this can't run on this machine

Confirmed, not assumed — two independent, unpatchable issues:

1. `import airflow` → works (with a warning), but `airflow db migrate` /
   `airflow dags test` crash with
   `AttributeError: module 'os' has no attribute 'register_at_fork'`
   (`airflow/_shared/observability/metrics/stats.py`) — a POSIX-only API
   Airflow's core calls unconditionally.
2. Even a plain module import of the DAG file fails separately with
   `ImportError: cannot import name 'ObjectStoragePath' from 'airflow.sdk'`
   — a broken internal import chain in `airflow.sdk` on this platform.

Apache Airflow does not officially support native Windows for exactly this
class of reason. The DAG code itself is ordinary, correct TaskFlow API — it
needs a Linux runtime, same as the course's own lab notebooks.

### Running it in Google Colab

```bash
# 1. Install Airflow (official constraints-pinned method) + this project's deps
AIRFLOW_VERSION=3.3.0
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt

# 2. Start a local Kafka broker (same approach as the Day 2 lab — KRaft, no ZK/Docker)
curl -sSOL https://downloads.apache.org/kafka/4.2.1/kafka_2.13-4.2.1.tgz
tar -xzf kafka_2.13-4.2.1.tgz
cd kafka_2.13-4.2.1
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -t $CLUSTER_ID -c config/server.properties --standalone
nohup bin/kafka-server-start.sh config/server.properties > /content/kafka.log 2>&1 &
sleep 10
for t in university.documents.raw university.documents.validated university.documents.dlq; do
  bin/kafka-topics.sh --create --topic $t --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
done
cd /content/ai-university-knowledge-hub   # back to the repo root

# 3. Point Airflow at this repo's airflow/ folder and initialize its metadata DB
export AIRFLOW_HOME=/content/ai-university-knowledge-hub/airflow
airflow db migrate

# 4. Seed sample documents (first run only) and execute the DAG
python scripts/seed_sample_documents.py
airflow dags test university_knowledge_hub_pipeline 2026-01-01
```

### Demonstrating the quality-gate halt (failure path, not just the happy path)

```bash
# Temporarily leave only the two known-bad fixtures in incoming_documents/,
# so every message gets rejected and the gate legitimately fails:
mkdir /tmp/good_docs_backup
mv incoming_documents/*.txt incoming_documents/*.docx incoming_documents/*.pdf /tmp/good_docs_backup/ 2>/dev/null
# (corrupted_upload.exe and empty_handbook.txt remain)

airflow dags test university_knowledge_hub_pipeline 2026-01-02
# Expect: check_ingestion_quality raises AirflowException (accepted=0);
# bronze_load / silver_transform / gold_aggregate show upstream_failed —
# never executed on the empty batch.

# Restore the good fixtures afterward:
mv /tmp/good_docs_backup/* incoming_documents/
```

## Stage 3 — RAG pipeline (done)

Real ChromaDB (HNSW), `rank_bm25`, and `sentence-transformers`
(bi-encoder + cross-encoder). Answers are grounded in retrieved context
with citations, or the pipeline explicitly refuses — it never hallucinates.

```
Silver (Delta, via deltalake — no Spark needed for RAG)
    -> rag/chunking/chunker.py            sentence-aligned overlapping chunks
    -> rag/vector_db/chroma_store.py      all-MiniLM-L6-v2 embeddings -> ChromaDB (HNSW)
    -> rag/retrieval/bm25_index.py        keyword index (rank_bm25)
    -> rag/retrieval/hybrid_search.py     Reciprocal Rank Fusion (k=60) of both
    -> rag/reranker/cross_encoder_reranker.py   ms-marco-MiniLM-L-6-v2 rerank to top-3
    -> rag/generation/prompt_builder.py   numbered, citable context + strict grounding prompt
    -> rag/generation/answer_generator.py grounded answer, or refusal if ungrounded
```

**LLM**: a local, open-source instruct model (`Qwen/Qwen2.5-1.5B-Instruct`
via `transformers`, CPU) — no API key required, per project decision. What
actually prevents hallucination is the system prompt's hard constraint
("answer ONLY from CONTEXT, otherwise say the exact refusal sentence"), not
the specific model; swapping in a larger/hosted model is a one-line change
in `GENERATION_MODEL`.

### Running it

```bash
.venv/Scripts/python.exe rag/rag_pipeline.py
```

### Proven evidence (see `logs/rag_pipeline.log`)

All 7 example questions from the brief, run end-to-end through the real
pipeline (chunking → embed → hybrid search → rerank → generate):

| Question | Answer grounded correctly? | Citation |
|---|---|---|
| What are the graduation requirements? | ✅ 132 credit hours, 30 in residence, capstone | `graduation_requirements.txt` |
| What courses are required for Computer Science? | ✅ full CS 101–320 course list | `cs_program_requirements.docx` |
| How many credit hours are needed to graduate? | ✅ 132 | `graduation_requirements.txt` |
| What is the deadline for course withdrawal? | ✅ Week 10 (15-week) / Week 5 (8-week) | `course_withdrawal_policy.txt` |
| What is the attendance policy? | ✅ 20% absence threshold, WF grade | `attendance_policy.pdf` |
| What scholarships are available? | ✅ Presidential/Dean's/Departmental/Athletic | `scholarships_financial_aid.txt` |
| *What is the university's parking permit policy?* | **Correctly refused** — no document covers it | *(none)* |

The refusal case is the important one: hybrid search still returned 6
candidates (nothing in the KB is truly empty), but the cross-encoder/LLM
correctly recognized none of them answer a parking question and replied
exactly `"I don't know based on the available university documents."` with
no citations — proving the grounding constraint works even when retrieval
returns unrelated results, not just when retrieval returns nothing.

## Stage 5 — Quality gate + Lineage (done)

Real Great Expectations 1.x (fluent API) and real `openlineage-python`
(`FileTransport`, no Marquez server needed) — same real-library pattern as
the Day 4 lab, generalized to wrap every stage.

- `quality/ge_checkpoint.py` — a GX checkpoint against the *governed Silver
  table* (not Bronze): not-null keys, `file_type` in `{pdf,docx,txt}`,
  `word_count > 0`, `is_valid = true`, and — the important one —
  **`file_name` uniqueness**, which directly verifies the Stage 2 MERGE
  kept exactly one row per business key. This is a distinct quality layer
  from Stage 1's per-record Pydantic contract: it checks the *state* the
  pipeline produced, not each record on the way in.
- `lineage/lineage_emitter.py` — a reusable `emit_lineage(job_name)` context
  manager: real START before a stage, COMPLETE on success, FAIL (re-raised)
  on exception. One line wraps any stage.
- `scripts/run_pipeline_with_lineage.py` — runs the whole pipeline
  (ingestion → Bronze → Silver → **quality gate** → Gold) with lineage
  wrapping every stage and gating Gold on both the ingestion accept-count
  and the GX checkpoint.
- `airflow/dags/university_pipeline_dag.py` was updated with a
  `quality_checkpoint` task (gating `gold_aggregate`) and the same lineage
  wrapping, for when Stage 4 gets executed in Colab.

### Running it

```bash
.venv/Scripts/python.exe scripts/run_pipeline_with_lineage.py   # happy path
.venv/Scripts/python.exe scripts/demo_quality_gate_failure.py   # failure path
```

### Proven evidence

**Happy path** (`logs/pipeline_with_lineage.log`): all 7 stages
(`produce_documents`, `validate_and_consume`, `check_ingestion_quality`,
`bronze_load`, `silver_transform`, `quality_checkpoint`, `gold_aggregate`)
show a `[LINEAGE] START` immediately followed later by `[LINEAGE] COMPLETE`;
the GX checkpoint logs all 6 expectations as `PASSED`.

**Failure path** (`logs/quality_gate_failure_demo.log`) —
`scripts/demo_quality_gate_failure.py` deliberately injects a duplicate
`file_name` into Silver (simulating a MERGE bug) and proves, in one
self-contained, self-cleaning run:

1. GX genuinely detects it: `[FAILED] expect_column_values_to_be_unique
   (column=file_name)` — the other 5 expectations still pass.
2. A real OpenLineage **FAIL** event is emitted for `quality_checkpoint`.
3. `gold_aggregate was called: False` — asserted in code, not just logged —
   the gate actually blocked Gold, it didn't just complain about it.
4. The injected row is deleted and Silver is verified back at 5 rows
   afterward, so this demo doesn't leave the Lakehouse corrupted.

## All 5 capstone deliverables are now built and evidenced

| # | Deliverable | Pts | Status |
|---|---|---|---|
| 1 | Ingestion | 20 | ✅ Done, verified |
| 2 | Delta Lakehouse | 25 | ✅ Done, verified |
| 3 | RAG Pipeline | 25 | ✅ Done, verified |
| 4 | Orchestration | 15 | 🟡 DAG complete; needs a Colab run for execution evidence (Airflow doesn't run on native Windows — see above) |
| 5 | Quality Gate + Lineage | 15 | ✅ Done, verified (happy + failure path) |

The only open item is actually executing Stage 4's DAG in Colab (or
WSL2/Docker) to capture `airflow dags test` output — everything else has
been run and its output captured in `logs/`.
