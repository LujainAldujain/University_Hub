# 🎓 AI University Knowledge Hub

> **Modern Data Engineering for AI Systems -- Capstone Project**\
> SDAIA Academy \| Learning Space

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Kafka](https://img.shields.io/badge/Kafka-Streaming-black)
![Delta](https://img.shields.io/badge/Delta-Lakehouse-green)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-red)
![RAG](https://img.shields.io/badge/RAG-Hybrid_Search-orange)

------------------------------------------------------------------------


---

# 👥 Project Team

This capstone project was developed collaboratively by:

| Student |
|----------|
| **Sarah Albukhaytan** |
| **Lujain Aldujain** |
| **Ayidah Alswayed** |

---

# 📖 Overview

AI University Knowledge Hub is an end-to-end data engineering pipeline
that ingests university documents, validates incoming data, stores it in
a Delta Lakehouse (Bronze/Silver/Gold), and enables Retrieval-Augmented
Generation (RAG) for answering student questions using trusted
institutional documents.

## ✨ Features

-   Kafka Producer & Consumer
-   Pydantic Schema Validation
-   Dead-letter handling
-   Delta Lake Bronze/Silver/Gold
-   MERGE (Upsert)
-   Great Expectations Quality Gate
-   OpenLineage Tracking
-   Apache Airflow DAG
-   Document Chunking
-   Embeddings
-   ChromaDB Vector Store
-   Hybrid Search (Dense + BM25)
-   Cross Encoder Reranking
-   Grounded Answers with Citations

------------------------------------------------------------------------

# 🏗️ Architecture

``` 
flowchart LR

A[University Documents]
-->B(Kafka)

B-->C[Schema Validation]

C-->D[Bronze]

D-->E[Silver]

E-->F[Gold]

F-->G[Chunking]

G-->H[Embeddings]

H-->I[(ChromaDB)]

User-->J(Query)

J-->K(Hybrid Search)

K-->L(Cross Encoder)

L-->M(LLM)

I-->K

M-->N(Grounded Answer)
```

# ⚙️ Technology Stack

  Layer             Technology
  ----------------- ------------------------------
  Streaming         Apache Kafka
  Validation        Pydantic
  Processing        PySpark
  Lakehouse         Delta Lake
  Orchestration     Apache Airflow
  Quality           Great Expectations
  Lineage           OpenLineage
  Vector Database   ChromaDB
  Embeddings        Sentence Transformers
  Retrieval         Hybrid Search (Dense + BM25)
  Reranking         Cross Encoder

# 📂 Project Structure

``` text
University_Hub/
├── notebooks/
├── scripts/
├── dags/
├── data/
│   ├── bronze
│   ├── silver
│   └── gold
├── incoming_documents/
├── requirements.txt
└── README.md
```

# 🚀 Pipeline

## Stage 1 -- Kafka Ingestion

Incoming university documents are streamed through Kafka. Every message
is validated using Pydantic. Invalid records are routed to a
quarantine/dead-letter area.

**Expected Output** - Valid records accepted - Invalid records rejected
with reason

------------------------------------------------------------------------

## Stage 2 -- Delta Lakehouse

  Layer    Purpose
  -------- --------------------------
  Bronze   Raw ingested data
  Silver   Cleaned & validated data
  Gold     Aggregated analytics

Schema enforcement and MERGE operations preserve data quality.

------------------------------------------------------------------------

## Stage 3 -- RAG Pipeline

1.  Load documents
2.  Chunk text
3.  Generate embeddings
4.  Store vectors in ChromaDB
5.  Hybrid Search (Dense + BM25)
6.  Cross Encoder reranking
7.  LLM generates grounded answer with citations

Example:

**Question**

> How many credit hours are required for graduation?

**Answer**

> 132 credit hours.

------------------------------------------------------------------------

## Stage 4 -- Airflow

The Airflow DAG orchestrates the complete workflow.

``` text
Kafka
 ↓
Validation
 ↓
Bronze
 ↓
Silver
 ↓
Gold
 ↓
RAG
```

------------------------------------------------------------------------

## Stage 5 -- Quality & Lineage

Great Expectations validates data before downstream execution.

OpenLineage emits START / COMPLETE / FAIL events for every stage.

------------------------------------------------------------------------

# ▶️ Run

Directe by:
https://githubtocolab.com/LujainAldujain/University_Hub/blob/main/notebooks/capstone_demo.ipynb


OR

## How to Run

Open a new Google Colab notebook and run the following cells **in order,
within a single session** (Colab resets everything — Airflow, Kafka,
installed packages — if the runtime restarts, so all steps must be re-run
together after any disconnect).

### 1. Clone the repo

\`\`\`python
!git clone https://github.com/LujainAldujain/University_Hub.git /content/ai-university-knowledge-hub
%cd /content/ai-university-knowledge-hub
\`\`\`

### 2. Install Airflow and project dependencies

\`\`\`python
%%bash
AIRFLOW_VERSION=3.3.0
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r requirements.txt
\`\`\`

### 3. Start a local Kafka broker (KRaft mode, no ZooKeeper/Docker needed)

\`\`\`python
%%bash
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
\`\`\`

### 4. Point Airflow at this repo and initialize its metadata DB

\`\`\`python
%%bash
cd /content/ai-university-knowledge-hub
export AIRFLOW_HOME=/content/ai-university-knowledge-hub/airflow
airflow db migrate
\`\`\`

### 5. Seed sample documents and run the DAG

\`\`\`python
%%bash
cd /content/ai-university-knowledge-hub
export AIRFLOW_HOME=/content/ai-university-knowledge-hub/airflow
python scripts/seed_sample_documents.py
airflow dags test university_knowledge_hub_pipeline 2026-01-01
\`\`\`

A successful run ends with:

\`\`\`
Dag run in success state
DagRun Finished: dag_id=university_knowledge_hub_pipeline, ... state=success
\`\`\`

# ✅ Capstone Deliverables

  Requirement           Status
  -------------------- --------
  Kafka Ingestion         ✅
  Schema Validation       ✅
  Delta Lake              ✅
  Bronze/Silver/Gold      ✅
  MERGE                   ✅
  Airflow                 ✅
  Great Expectations      ✅
  OpenLineage             ✅
  Hybrid Search           ✅
  ChromaDB                ✅
  Cross Encoder           ✅


# 🙏 Acknowledgements

Developed as part of the **Modern Data Engineering for AI Systems**
capstone at SDAIA Academy.

Official GitHub: https://github.com/SDAIAAcademy

