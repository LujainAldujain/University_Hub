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

https://githubtocolab.com/LujainAldujain/University_Hub/blob/main/notebooks/capstone_demo.ipynb

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

