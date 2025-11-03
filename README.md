# Retrieval-Augmented Generation (RAG) Pipeline using Apache Spark (Incremental)
**Author:** Simbarashe Chinomona

This project builds a **parallel, incremental RAG pipeline** in **Scala + Apache Spark**. It detects changed PDFs, **chunks** text, **embeds** new/updated chunks via **Ollama (nomic-embed-text or mxbai-embed-large)**, publishes a **Parquet/Delta snapshot**, and serves **top-K retrieval** at query time to ground answers from **llama3.1:8b**.

---

## 1) Quick Start

### Prereqs
- **Scala 2.12.18**, **sbt 1.9+**
- **Apache Spark 3.5.x** (local or cluster)
- **Java 11 or 17**
- **Ollama** running locally or on LAN (`OLLAMA_HOST`, models pulled: `nomic-embed-text`, `llama3.1:8b`)
- A directory of PDFs (MSR corpus or your own)

### Build
```bash
sbt clean compile
```

### Run
```bash
sbt clean compile run
```

### Test
```bash
sbt clean compile test
```

### Walkthrough link
```text
https://youtu.be/7UWk9HDR3M4?si=b_YZ-D1rGpUSW2-H
```

### 1️⃣ Add/Update Context (Incremental Snapshot)
```bash
sbt "run -- add-context --pdfRoot <path-to-pdfs> --outDir <output-dir> [--reducers N]"
```
- Detects **new/changed PDFs** by SHA-256 `contentHash`.
- Extracts text, **chunks with overlap**, embeds only **missing** vectors, and updates the **retrieval snapshot**.

### 2️⃣ Query (top-K retrieval + answer with llama3.1:8b)
```bash
sbt "run -- question 'What does MSR say about bug prediction?'"
```
- Embeds the question, retrieves **top-K** chunks from the published snapshot, then **Ollama.chat** generates the final answer.

---

## 2) Architecture & Data Flow

### Phase A — Incremental Context Creation
```
PDFs  ──▶ Spark scan ──▶ hash & diff (manifest) ──▶ parse + normalize ──▶ chunk(overlap)
                 │                                           │
                 └───────────────────────────────────────────┴─▶ embed(new/missing) → L2 normalize
                                                                  │
                                                                  └─▶ publish: docs / chunks / embeddings → retrieval snapshot
```
Artifacts (high-level):
- **docs** — document metadata + `contentHash`
- **chunks** — paragraph/window chunks with stable IDs
- **embeddings** — vectors keyed by `(embedder, version)`
- **retrieval snapshot** — join of the latest docs + chunks + embeddings
- **metrics/manifest** — run stats + file hashes for deltas

### Phase B — Query & Answer
```
snapshot ─▶ embed(question) ─▶ cosine similarity ─▶ top-K context
                                                └─▶ Ollama.chat (llama3.1:8b) → grounded answer
```

---

## 3) How Each File Interacts (Module-by-Module Map)

### Core Packages
- **`config.AppConfig`** — central config: paths, models, Spark, retrieval params.
- **`helper.Pdf` / `helper.Chunker`** — PDF text extraction + paragraph/overlap chunking.
- **`helper.Vectors`** — L2 normalization and cosine utilities.
- **`models.Ollama`** — HTTP client for `/api/embeddings` and `/api/chat`.

### Incremental Builder
- **`IncrementalDatabasePipeline`** — manifest diff → chunk → embed (only missing) → publish snapshot (atomic).

### Query Path
- **`QueryMain`** — embed question → score cosine vs snapshot → pick top-K → call chat model.

### Entrypoint
- **`main.scala`** — CLI dispatcher:
  - `add-context` → incremental builder
  - `question` → retrieval + chat

---

## 4) Configuration

- **Models & paths** come from `application.conf`:
  - `paths.pdfRoot`, `paths.outDir`
  - `models.embedModel`, `models.chatModel`
  - `query.topK`, `spark.master`, shuffle partitions, etc.
- **Environment**:
  - `OLLAMA_HOST` (default `http://127.0.0.1:11434`)
  - AWS env (only if running on EMR/S3)

---

## 5) Implementation Notes (aligns to HW 2 criteria)
- **Incrementality** — only re-chunk/re-embed when `contentHash` changes.
- **Idempotency** — MERGE/replace patterns to avoid duplicates on retries.
- **Versioned embeddings** — `(embedder, version)` supports model upgrades side-by-side.
- **Atomic snapshot** — “create or replace” retrieval index for consistent queries.
- **Logging & metrics** — throughput, dedup ratio, elapsed time; verify “no-work” reruns.

---

## 6) Troubleshooting

- **Ollama not reachable**  
  Start daemon and pull models:
  ```bash
  ollama pull nomic-embed-text
  ollama pull llama3.1:8b
  ```
- **Slow embeddings locally**  
  Limit parallelism/batch size so executors don’t stampede the Ollama server.
- **No work on rerun**  
  Expected if hashes didn’t change (confirms delta behavior).
- **Windows oddities**  
  Use consistent paths; avoid mixing Hadoop and `java.nio` where not needed.

---

## 7) Example End-to-End
```bash
# 1) Build incremental snapshot (4 reducers)
sbt "run -- add-context --pdfRoot C:/data/MSRCorpus --outDir C:/tmp/hw2-out --reducers 4"

# 2) Ask a question
sbt "run -- question 'What does section 3 say about inconsistency vs consistency?'"

# 3) Re-run add-context without changes (should do near-zero work)
sbt "run -- add-context --pdfRoot C:/data/MSRCorpus --outDir C:/tmp/hw2-out"
```

---

## 8) Optional: AWS EMR (IaC)
- Provide **CloudFormation/Terraform** to spin up EMR + S3 + IAM.
- Upload the assembled JAR and run:
  ```bash
  spark-submit --class main s3://<bucket>/jars/hw2.jar     --add-context --pdfRoot s3://<bucket>/pdfs --outDir s3://<bucket>/out
  ```
- Keep logs in CloudWatch/S3; store configs in `application.conf`.

---
