# RAG Pipeline Homework 2

## Overview
This project incrementally builds and queries a retrieval-augmented generation (RAG) index from a corpus of PDF documents. The `add-context` flow ingests new or updated PDFs, chunks their text, embeds each chunk with an Ollama embedding model, and publishes a Parquet snapshot optimized for similarity search. The `question` flow embeds a user query, retrieves the most similar chunks from the snapshot, and sends the compiled context plus the question to an Ollama chat model for answer generation.【F:src/main/scala/main.scala†L8-L104】【F:src/main/scala/ragPipeline/Query/main.scala†L1-L132】

### Architecture
1. **Change detection** – PDFs under `app.pdfRoot` are hashed to identify new or modified documents compared to the prior manifest.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L205-L268】
2. **Text extraction & chunking** – Changed PDFs are parsed with PDFBox, normalized, and split into overlapping windows sized by `app.chunking.windowChars` and `app.chunking.overlapPct`.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L270-L356】
3. **Embedding generation** – Newly produced chunks are embedded in batches through the Ollama HTTP API using the configured embedder model and version; vectors are L2-normalized before storage.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L380-L448】
4. **Snapshot publishing** – Document metadata, chunks, embeddings, and the retrieval index are written to Parquet with deterministic partitioning so incremental runs only touch affected partitions.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L332-L351】【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L413-L452】【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L500-L526】
5. **Querying** – Retrieval loads the Parquet index, scores cosine similarity against the broadcast query embedding, and submits the top-k context to the chat model for answer generation.【F:src/main/scala/ragPipeline/Query/main.scala†L61-L132】【F:src/main/scala/ragPipeline/Query/main.scala†L139-L199】

## Prerequisites
- **Java**: JDK 11 or 17 (Spark 3.5.x supports either). Ensure `JAVA_HOME` is set.
- **sbt**: 1.9+ to build and run the Scala project defined in `build.sbt` (Scala 2.12.18, Spark 3.5.1).【F:build.sbt†L1-L44】
- **Apache Spark**: Included via sbt dependencies; no separate install is needed for local mode.
- **Ollama**: Install and run the Ollama server so the pipeline can call the embedding and chat endpoints configured in `application.conf`.
- **Models**: Pull the following Ollama models before running the pipeline:
  - `nomic-embed-text` for embeddings.【F:src/main/resources/application.conf†L21-L28】
  - `llama3.1:8b` for question answering.【F:src/main/resources/application.conf†L21-L37】

Start the Ollama daemon (defaults to `http://127.0.0.1:11434`) before running any sbt commands that embed or chat. You can override the base URL and model names through `application.conf` or JVM system properties (e.g., `-Drag.embedModel=...`).【F:src/main/resources/application.conf†L29-L37】【F:src/main/scala/ragPipeline/Query/main.scala†L45-L58】

## Data Layout
- **Input**: Place PDFs under the directory configured as `app.pdfRoot` (defaults to `data/MSRCorpus`).【F:src/main/resources/application.conf†L7-L14】
- **Output root**: `app.outDir` (defaults to `out/`). The incremental builder creates:
  - `out/doc_normalized/`: Document-level metadata partitioned by `(shard, docId)` and versioned append-only.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L332-L351】
  - `out/chunks/`: Chunk text and boundaries partitioned by `(shard, docId)`.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L332-L351】
  - `out/embeddings/`: Vector store partitioned by `(embedder, embVersion, shard, docId)` so multiple embedding versions can coexist.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L413-L452】
  - `out/retrieval_index/`: Query-ready snapshot partitioned by `(shard, docId)` that joins the latest chunk metadata with embeddings.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L500-L526】
  - `out/_manifest/`: File manifest storing size, mtime, and SHA-256 hashes for incremental change detection.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L185-L241】
  - `out/_metrics/` & `out/run_metrics.csv`: Run metrics keeping the latest two runs for quick inspection.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L57-L146】

Each run only reprocesses PDFs whose content hash changed, minimizing redundant work and preserving historical versions of documents, chunks, and embeddings.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L205-L409】

## Key Source Files
- `src/main/scala/main.scala` – CLI entry point that parses `add-context` and `question` subcommands, injects configuration, and invokes the incremental builder or query runner.【F:src/main/scala/main.scala†L8-L104】
- `src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala` – Orchestrates incremental ingest: manifest comparison, chunk extraction, embedding, and Parquet publishing for new or updated PDFs.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L57-L526】
- `src/main/scala/ragPipeline/helper/Chunker.scala` – Implements PDF text normalization, windowing, and overlap logic used during incremental updates.【F:src/main/scala/ragPipeline/helper/Chunker.scala†L1-L116】
- `src/main/scala/ragPipeline/Query/main.scala` – Embeds user queries, retrieves similar chunks from the published snapshot, and streams LLM answers.【F:src/main/scala/ragPipeline/Query/main.scala†L26-L199】
- `src/main/resources/application.conf` – Centralizes runtime configuration: PDF roots, output directories, model names, retrieval parameters, and Spark defaults.【F:src/main/resources/application.conf†L1-L47】

## Building the Project
```bash
cd Fall2025_HW2
sbt compile
```
This downloads all Scala/Spark dependencies and ensures sources compile. Use `sbt test` to run the available unit tests.【F:build.sbt†L1-L44】

## Running the Incremental Builder (`add-context`)
1. Ensure the Ollama server is running and the required models are pulled.
2. Place PDFs under the configured `app.pdfRoot`.
3. From the project root run:
   ```bash
   sbt "run -- add-context --pdfRoot <input_pdf_dir> --outDir <output_dir> [--reducers N]"
   ```
   - All flags are optional; defaults come from `application.conf`.
   - `--pdfRoot` accepts local or S3/HDFS URIs supported by Hadoop.
   - `--outDir` controls where the Parquet outputs, manifest, and metrics land.
   - `--reducers` tunes Spark shuffle parallelism when running on a cluster.

The driver ensures the local directories exist before Spark writes and then dispatches the incremental pipeline. Re-running the command only re-embeds changed PDFs, updates affected partitions, and refreshes the retrieval snapshot.【F:src/main/scala/main.scala†L16-L68】【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L205-L526】

### Customizing via JVM Properties
You can override configuration keys without editing `application.conf` by passing `-D` properties to sbt. For example:
```bash
sbt "-Drag.embedModel=mxbai-embed-large" "-Dspark.master=local[4]" "run -- add-context"
```
Available overrides include model names, output paths, top-k retrieval count, and Spark master URL.【F:src/main/scala/ragPipeline/Query/main.scala†L45-L83】【F:src/main/resources/application.conf†L7-L47】

## Running Queries (`question`)
After building the context database, ask questions with:
```bash
sbt "run -- question '<your question here>'"
```
- If you pass two or more arguments, the first is treated as the shards/output directory (e.g., `out/`), and the remaining text becomes the question.
- At runtime the query job locates the retrieval snapshot in the following priority: `-Drag.indexTable`, `-Drag.indexPath`, `app.paths.retrievalIndexDir`, or `<shardsParent>/retrieval_index`.【F:src/main/scala/ragPipeline/Query/main.scala†L26-L112】
- The job embeds the question via Ollama, computes cosine similarity against stored embeddings, prints the top-k context snippets, and streams the LLM answer to stdout.【F:src/main/scala/ragPipeline/Query/main.scala†L113-L199】

To switch between Spark and a MapReduce implementation, set `app.engine` in `application.conf` (`spark` by default). The driver routes questions to the appropriate query engine automatically.【F:src/main/scala/main.scala†L30-L104】【F:src/main/resources/application.conf†L3-L6】

## Conceptual Notes
- **Incrementality**: The manifest plus SHA-256 hashing guarantees only changed PDFs trigger re-chunking and re-embedding, preserving throughput on large corpora.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L205-L409】
- **Partition strategy**: Partitioning by shard and document ID keeps related rows co-located, enabling dynamic partition overwrite so incremental runs touch only the necessary Parquet partitions.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L332-L351】【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L500-L526】
- **Vector store schema**: Embeddings include embedder identifiers and version tags so you can roll out new models alongside old ones without rewriting history.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L413-L452】
- **Model integration**: The Ollama client centralizes HTTP calls for both embedding batches and chat completions, and the query path uses the same embedder to keep the vector space aligned.【F:src/main/scala/ragPipeline/models/Ollama.scala†L18-L150】【F:src/main/scala/ragPipeline/Query/main.scala†L45-L132】

## Troubleshooting
- **Ollama connection errors**: Confirm the daemon is running on the host specified by `app.models.ollama.baseUrl` and that the models are pulled locally.【F:src/main/resources/application.conf†L29-L37】
- **Empty query embeddings**: The query job exits if the embedder returns an empty vector; double-check the embed model name and server logs.【F:src/main/scala/ragPipeline/Query/main.scala†L133-L158】
- **Spark master**: For local development the default `local[*]` master is used; configure `-Dspark.master` when submitting to a cluster.【F:src/main/scala/ragPipeline/Query/main.scala†L71-L75】

## Testing & Verification
Run the automated suite with:
```bash
sbt test
```
Additional manual verification includes inspecting the Parquet outputs under `out/` and reviewing the most recent run metrics CSV for ingest statistics.【F:src/main/scala/ragPipeline/IncrementalDatabaseCreation/IncrementalDatabasePipeline.scala†L57-L146】
