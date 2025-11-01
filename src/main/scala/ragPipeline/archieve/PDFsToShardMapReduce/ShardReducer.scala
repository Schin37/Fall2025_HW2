package ragPipeline.archieve.PDFsToShardMapReduce

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicReference
import io.circe.parser._
import org.apache.commons.csv.{CSVFormat, CSVPrinter, QuoteMode}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HPath}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Reducer, lib}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig, VectorSimilarityFunction}
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.document.KnnFloatVectorField
//import org.apache.lucene.document.VectorSimilarityFunction
import org.slf4j.LoggerFactory
import ragPipeline.helper.VocabAccumulator

final class ShardReducer extends Reducer[IntWritable, Text, Text, Text] {

  private val log = LoggerFactory.getLogger(getClass)
  private val resRef = new AtomicReference[Resources](null)

  private def res: Resources = {
    val r = resRef.get()
    require(r != null, "Reducer resources not initialized; setup() not executed?")
    r
  }

  override def setup(ctx: Reducer[IntWritable, Text, Text, Text]#Context): Unit = {
    val attempt = ctx.getTaskAttemptID.toString
    val fs: FileSystem = FileSystem.get(ctx.getConfiguration)
    val outDir: HPath = FileOutputFormat.getOutputPath(ctx)
    val acc = new VocabAccumulator
    resRef.set(Resources(fs, outDir, attempt, acc))
  }

  override def reduce(
                       key: IntWritable,
                       values: java.lang.Iterable[Text],
                       ctx: Reducer[IntWritable, Text, Text, Text]#Context
                     ): Unit = {

    val R = res
    val shard = key.get
    val local = Files.createTempDirectory(s"lucene-shard-$shard")

    val iwc = new IndexWriterConfig(new StandardAnalyzer())
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    val fsDir = FSDirectory.open(local)
    val iw = new IndexWriter(fsDir, iwc)

    import scala.collection.JavaConverters._

    values.asScala.foreach { t =>
      val json = t.toString
      try {
        parse(json) match {
          case Left(err) =>
            ctx.getCounter("HW1", "REDUCE_BAD_JSON").increment(1)
            log.info(s"[Reducer] Skipping unparsable JSON: ${err.getMessage}")

          case Right(js) =>
            val c = js.hcursor
            val parsed = for {
              docId <- c.get[String]("doc_id").toOption
              chunkId <- c.get[Int]("chunk_id").toOption
              text <- c.get[String]("text").toOption
              vec <- c.get[Vector[Float]]("vec").toOption
              if vec.nonEmpty && vec.forall(v => !v.isNaN && !v.isInfinity)
            } yield (docId, chunkId, text, vec)

            parsed match {
              case Some((docId, chunkId, rawText, vec)) =>
                val text = normalize(rawText)
                if (looksJunk(text)) {
                  ctx.getCounter("HW1", "REDUCE_JUNK_CHUNKS").increment(1)
                  if ((chunkId % 50) == 0)
                    log.info(s"[Reducer] Dropped junk chunk #$chunkId doc=$docId text='${text.take(80)}'")
                } else {
                  val csv = ensureDocs(R)
                  csv.printRecord(docId, chunkId.toString, text)

                  R.acc.addChunk(text)

                  val doc = new Document()
                  doc.add(new StringField("doc_id", docId, Field.Store.YES))
                  doc.add(new StringField("chunk_id", chunkId.toString, Field.Store.YES))
                  doc.add(new StringField("text", text, Field.Store.YES))

                  val score = new KnnFloatVectorField("vec", vec.toArray, VectorSimilarityFunction.COSINE)
                  doc.add(score)
                  iw.addDocument(doc)

                  val safe = (s: String) =>
                    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
                  val jsonLine =
                    s"""{"shard":$shard,"doc_id":${safe(docId)},"chunk_id":$chunkId,"text_len":${text.length}}"""

                  ctx.write(new Text(s"doc:$docId#$chunkId"), new Text(jsonLine))
                  ctx.getCounter("HW1", "REDUCE_WRITES").increment(1)
                }

              case None =>
                ctx.getCounter("HW1", "REDUCE_BAD_FIELDS").increment(1)
                log.info(s"[Reducer] Skipping row with missing/invalid fields: ${json.take(200)}...")
            }
        }
      } catch {
        case e: Throwable =>
          ctx.getCounter("HW1", "REDUCE_UNCAUGHT").increment(1)
          log.error(s"[Reducer] Uncaught while handling row: ${e.getMessage}")
      }
    }

    iw.commit()
    iw.close()
    fsDir.close()

    // Copy local Lucene index to HDFS
    val conf = ctx.getConfiguration
    val hdfsFs = FileSystem.get(conf)
    val localFs = FileSystem.getLocal(conf)
    val outRoot: HPath = FileOutputFormat.getOutputPath(ctx)
    val dest = new HPath(outRoot, s"index_shard_$shard")
    val src = new HPath(local.toUri)

    hdfsFs.mkdirs(dest.getParent)
    FileUtil.copy(localFs, src, hdfsFs, dest, false, true, conf)
  }

  override def cleanup(ctx: Reducer[IntWritable, Text, Text, Text]#Context): Unit = {
    val R = res
    R.docCsv.foreach(_.flush())
    R.docWriter.foreach(_.flush())
    R.docWriter.foreach(_.close())

    if (!R.acc.isEmpty) {
      val vw = ensureVocab(R)
      R.acc.writeCsv(line => {
        vw.write(line)
        vw.newLine()
      }, includeHeader = false)
      vw.flush()
      vw.close()
    }
  }

  private def normalize(s: String): String =
    s.replaceAll("\\s+", " ").trim

  private def looksJunk(s: String): Boolean = {
    val t = normalize(s)
    val tooShort = t.length < 40
    val pageLabel = t.matches("""^[\W_]*\d{1,4}[\W_]*$""")
    val toks = t.split("\\s+")
    val mostlyNums = toks.nonEmpty && toks.count(w => w.forall(_.isDigit)) >= (0.6 * toks.length)
    tooShort || pageLabel || mostlyNums
  }

  private def ensureDocs(R: Resources): CSVPrinter = {
    R.docCsv.getOrElse {
      val docsPath = new HPath(R.outDir, s"csv-attempt_docs_${R.attempt}.csv")
      val docsOut = R.fs.create(docsPath, true)
      val dw = new BufferedWriter(new OutputStreamWriter(docsOut, StandardCharsets.UTF_8))
      val csv = new CSVPrinter(
        dw,
        CSVFormat.DEFAULT
          .builder()
          .setHeader("doc_id", "chunk_id", "text")
          .setQuoteMode(QuoteMode.MINIMAL)
          .build()
      )
      R.docWriter = Some(dw)
      R.docCsv = Some(csv)
      csv
    }
  }

  private def ensureVocab(R: Resources): BufferedWriter = {
    R.vocabWriter.getOrElse {
      val vocabPath = new HPath(R.outDir, s"csv-attempt_vocab_${R.attempt}.csv")
      val vocabOut = R.fs.create(vocabPath, true)
      val vw = new BufferedWriter(new OutputStreamWriter(vocabOut, StandardCharsets.UTF_8))
      R.vocabWriter = Some(vw)
      vw
    }
  }
}

final case class Resources(
  fs: FileSystem,
  outDir: HPath,
  attempt: String,
  acc: VocabAccumulator,
  var docCsv: Option[CSVPrinter] = None,
  var docWriter: Option[BufferedWriter] = None,
  var vocabWriter: Option[BufferedWriter] = None
)
