# Retrieval-Augmented Generation (RAG) Incremental Pipeline
**Author:** Simbarashe Chinomona

This homework implements a **Spark-based incremental RAG pipeline** in **Scala**. It watches a PDF corpus, detects changes, **chunks** new pages, **embeds** them with **Ollama (`nomic-embed-text`)**, publishes **Parquet shards for retrieval**, and serves **top-K answers** through either the Spark query job or a MapReduce compatibility runner that calls **`llama3.1:8b`** for grounded responses.

---

## 1) Quick Start

### Prereqs
- **Java 11 or 17** (set `JAVA_HOME`).
- **sbt 1.9+** (Scala 2.12.18 project targeting Spark 3.5.1).【F:build.sbt†L1-L44】
- **Apache Spark 3.5.x** (brought in via sbt when running locally).
- **Hadoop client binaries** (Spark submits will look for `HADOOP_HOME` when targeting HDFS/S3).
- **Ollama** daemon running on the machine (default `http://127.0.0.1:11434`).
- **Models to pull before first run:**
  ```bash
  ollama pull nomic-embed-text
  ollama pull llama3.1:8b
  ```
- **Corpus:** place PDFs in `data/MSRCorpus/` or pass `--pdfRoot` to point at a different directory.【F:src/main/resources/application.conf†L7-L37】

### Build
```bash
sbt clean compile
```

### Test
```bash
sbt test
```

### Interactive Run (prompted menu)
```bash
sbt clean compile run
```
When prompted choose `1` for **add-context** or `2` for **question**.【F:src/main/scala/main.scala†L25-L68】

### 1️⃣ Publish / Refresh the Context Snapshot
```bash
sbt "run -- add-context --pdfRoot <path-to-pdfs> --outDir <output-root> [--reducers N]"
```
- Missing flags fall back to `application.conf` defaults (`data/MSRCorpus`, `out/`).【F:src/main/scala/main.scala†L50-L83】【F:src/main/resources/application.conf†L7-L37】
- Each run hashes PDFs, only re-chunks changed documents, and repopulates affected Parquet partitions for documents, chunks, embeddings, and the retrieval index.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L185-L526】
- Metrics for the most recent two runs land in `out/_metrics/` plus `out/run_metrics.csv` for quick inspection.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L57-L146】

### 2️⃣ Ask Questions Against the Snapshot
```bash
sbt "run -- question [<shards-parent>] <question text...>"
```
- If you pass only the question, the job looks in `out/retrieval_index/`. Provide a first argument to point at another snapshot directory.【F:src/main/scala/main.scala†L68-L103】【F:src/main/scala/ragPipeline/Query/main.scala†L15-L112】
- Set JVM props to tweak behavior, e.g. `-Drag.topK=8`, `-Drag.indexPath=/path/to/retrieval_index`, `-Drag.embedModel=mxbai-embed-large`.
- Answers print top-K supporting chunks followed by the llama response in the console.【F:src/main/scala/ragPipeline/Query/main.scala†L113-L199】

---

## 2) Architecture & Data Flow

### Phase A — Incremental Context Build
```
PDFs ──▶ Manifest scanner ──▶ Chunker ──▶ Ollama.embed ──▶ Parquet writers
             │                     │                     ├─▶ out/chunks/
             │                     │                     ├─▶ out/embeddings/
             │                     │                     └─▶ out/retrieval_index/
             └─▶ Tracks SHA-256 + size/mtime → skips unchanged docs
```
Key behaviors:
- Manifest in `out/_manifest/` stores hashes and metadata for change detection.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L185-L332】
- Chunking uses sliding windows (`windowChars`, `overlapPct`) to produce context-friendly spans.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L270-L356】
- Embeddings are normalized, version-tagged, and partitioned by `(embedder, embVersion, shard, docId)` so you can roll out new models side-by-side.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L380-L452】
- Retrieval snapshot joins the freshest chunks + embeddings into `out/retrieval_index/` for fast Spark reads.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L500-L526】

### Phase B — Query & Answer
```
retrieval_index/ ─▶ Spark query job ─▶ cosine similarity ─▶ top-K context ─▶ Ask.scala ─▶ Ollama.chat
```
- The query job embeds the question, broadcasts the vector, and scores cosine similarity per chunk.【F:src/main/scala/ragPipeline/Query/main.scala†L61-L132】
- Top-K hits are formatted with doc metadata, streamed to stdout, and fed into `Ask.buildMessages` before hitting the chat endpoint.【F:src/main/scala/ragPipeline/Query/main.scala†L133-L199】【F:src/main/scala/ragPipeline/models/Ask.scala†L1-L117】
- Set `app.engine = "mr"` in `application.conf` to route questions through the MapReduce compatibility layer (`QueryEngineMapReducer`).【F:src/main/scala/main.scala†L34-L66】【F:src/main/resources/application.conf†L3-L6】

---

## 3) Files to Focus On

### Command Driver & Configuration
- `src/main/scala/main.scala` — CLI entry point; wires add-context vs question modes and applies config overrides.【F:src/main/scala/main.scala†L8-L104】
- `src/main/resources/application.conf` — Central defaults: directories, engine toggle, model names, chunk sizing, MR reducer counts.【F:src/main/resources/application.conf†L1-L75】

### Incremental Pipeline (core of HW2)
- `IncrementalDatabasePipeline.scala` — End-to-end incremental flow: manifest diff, chunk/embedding creation, Parquet publishing, metrics writing.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L1-L526】
- `IncrementalHelper.scala` — Utility helpers reused across pipeline stages (manifest, hashing, Spark I/O).【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalHelper.scala†L1-L224】
- `Embedder.scala` — Thin wrapper around Ollama embed API with batching, normalization, and back-off.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/Embedder.scala†L1-L189】
- `IncrementalDatabasePipeline2.scala` / `IncrementalDatabasePipeline3.scala` — Iteration snapshots kept for reference; focus on `IncrementalDatabasePipeline.scala` for the final flow.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline2.scala†L1-L447】【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline3.scala†L1-L517】

### Query Side
- `Query/main.scala` — Spark retrieval job that loads Parquet snapshots, computes similarity, and prompts the chat model.【F:src/main/scala/ragPipeline/Query/main.scala†L1-L199】
- `RelevantInfoMapReduce/` — Legacy MR jobs kept for grading; use when `app.engine = "mr"` or when targeting Hadoop clusters without Spark.【F:src/main/scala/ragPipeline/RelevantInfoMapReduce/ShardReducerRelevantInfo.scala†L1-L180】
- `models/` (`Ollama.scala`, `Ask.scala`) — HTTP client + prompt construction for embedding and chat operations.【F:src/main/scala/ragPipeline/models/Ollama.scala†L18-L150】【F:src/main/scala/ragPipeline/models/Ask.scala†L1-L117】

### Helpers
- `helper/Chunker.scala` — Sliding-window chunk creation and text cleanup.【F:src/main/scala/ragPipeline/helper/Chunker.scala†L1-L116】
- `helper/Vectors.scala` — Vector math utilities (normalization, distance helpers).【F:src/main/scala/ragPipeline/helper/Vectors.scala†L1-L112】
- `helper/Pdf.scala` — PDF extraction using PDFBox with password handling and heuristics.【F:src/main/scala/ragPipeline/helper/Pdf.scala†L1-L168】

---

## 4) Configuration Tips
- Override any key without editing the file using JVM props: `sbt "-Drag.embedModel=mxbai-embed-large" "-Dspark.master=local[4]" "run -- add-context"`.【F:src/main/scala/ragPipeline/Query/main.scala†L33-L83】
- `app.embed.batchSize` controls how many chunks per Ollama call; keep small if GPU RAM is limited.【F:src/main/resources/application.conf†L39-L52】
- Tune `app.chunking.windowChars` & `overlapPct` to adjust context length vs coverage.【F:src/main/resources/application.conf†L38-L60】
- Switch between Spark and MR execution by flipping `app.engine`. The driver automatically routes query requests accordingly.【F:src/main/scala/main.scala†L34-L66】

---

## 5) Implementation Notes
- **Incrementality**: SHA-256 + size/mtime manifest ensures only changed PDFs are reprocessed, keeping historical Parquet partitions intact.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L185-L332】
- **Partition Strategy**: Parquet datasets partition on shard & docId to keep related records co-located and minimize rewrite scope.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L332-L526】
- **Vector Storage**: Embeddings include `(embedder, embVersion)` columns so new models can roll out gradually.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L380-L452】
- **Metrics & Logging**: `RunMetrics` append-only parquet + CSV snapshots make grading easy; logs default to INFO via Logback.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L57-L146】【F:src/main/resources/application.conf†L70-L75】

---

## 6) Troubleshooting
- **Ollama connection errors** → Verify daemon is running on the host in `app.models.ollama.baseUrl` and models are pulled locally.【F:src/main/resources/application.conf†L29-L37】
- **Empty question embedding** → The job exits early; double-check the embed model name or server logs.【F:src/main/scala/ragPipeline/Query/main.scala†L102-L159】
- **Spark master issues** → Override with `-Dspark.master=local[*]` (default) or your cluster URL when submitting jobs.【F:src/main/scala/ragPipeline/Query/main.scala†L45-L83】
- **Stale outputs** → Set `app.run.deleteOutputIfExists=false` for debugging; defaults to true so each incremental run refreshes partitions safely.【F:src/main/resources/application.conf†L90-L96】

---

## 7) Example End-to-End Session
```bash
# 1) Build or refresh the snapshot (Spark local mode)
sbt "run -- add-context --pdfRoot data/MSRCorpus --outDir out"

# 2) Ask a question using the freshly built retrieval index
sbt "run -- question out 'What evidence is there about bug prediction?'"

# 3) Optional: Run unit tests
sbt test
```
Outputs land in `out/` (manifest, metrics, Parquet shards) and the console prints both supporting passages and the final llama answer.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L57-L526】【F:src/main/scala/ragPipeline/Query/main.scala†L113-L199】
