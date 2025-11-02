package ragPipeline.config

import com.typesafe.config.{Config, ConfigFactory}
//import org.apache.kerby.config.Config

import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._

/** Centralized, typed access to application.conf for HW1 / HW2. */
object AppConfig {

  // Load default app config; supports -Dconfig.resource=... overrides
  private val cfg: Config = ConfigFactory.load().getConfig("app")

  // ---- helpers ----
  private def path(key: String): Path = Paths.get(cfg.getString(key)).normalize()

  object engine {
    val engine: String = cfg.getString("engine")
    val deleteOutputIfExists: Boolean = cfg.getBoolean("run.deleteOutputIfExists")
  }

  // -------- Paths -------
  object paths {
    val pdfRoot: Path   = path("pdfRoot")
    val outDir: Path    = path("outDir")
    val retrievalIndexDir: Path  = path("retrievalIndexDir")
    val shardsDir: Path = path("shardsDir")
    val eachPartFileNamePrefix: String = cfg.getString("files.eachPartFilePrefix")
  }

  object FieldsName {
    val DocId:   String = cfg.getString("FieldsName.DocId")
    val ChunkId: String = cfg.getString("FieldsName.ChunkId")
    val Text:    String = cfg.getString("FieldsName.Text")
    val Vec:     String = cfg.getString("FieldsName.Vec")
  }

  // -------- Models & similarity --------
  object models {
    val embedModel: String = cfg.getString("models.embedModel")
    val chatModel:  String = cfg.getString("models.chatModel")
    val vectorDim:  Int    = cfg.getInt("models.vectorDim")
    val similarity: String = cfg.getString("models.similarity") // "cosine" | "dot" | "l2"
    def useCosine: Boolean = similarity.equalsIgnoreCase("cosine")
  }

  object embed {
    val concurrency = cfg.getInt("embed.concurrency")
    val batchSize   = cfg.getInt("embed.batchSize")
    val maxChars    = cfg.getInt("embed.maxChars")
    val minChunkChars = cfg.getInt("embed.minChunkChars")
  }

  // -------- Chunking --------
  object chunking {
    val windowChars: Int    = cfg.getInt("chunking.windowChars")
    val overlapPct:  Double = cfg.getDouble("chunking.overlapPct") // 0.10..0.25
    val overlapChars: Int   = math.round(windowChars * overlapPct.toFloat)
    val stride: Int         = (windowChars - overlapChars) max 1
    val maxContextBlock: Int = cfg.getInt("chunking.maxContextBlock")
    val NumShardBuckets: Int = cfg.getInt("chunking.NumShardBuckets")
  }

  // -------- MapReduce knobs --------
  object mr {
    val reducers: Int         = cfg.getInt("mr.reducers")
    val reducersContext: Int  = cfg.getInt("mr.reducersContext")
    val topK: Int             = cfg.getInt("mr.topK")
  }

  // -------- Tests / evaluation --------
  object tests {

    final case class SimPair(a: String, b: String)
    final case class Analogy(a: String, b: String, c: String, expected: String)

    val neighborsTopK: Int = cfg.getInt("tests.neighborsTopK")

    val wordSimilarity: Vector[SimPair] =
      cfg.getConfigList("tests.wordSimilarity").asScala.toVector.map { c =>
        SimPair(c.getString("a"), c.getString("b"))
      }

    val wordAnalogy: Vector[Analogy] =
      cfg.getConfigList("tests.wordAnalogy").asScala.toVector.map { c =>
        Analogy(
          c.getString("a"),
          c.getString("b"),
          c.getString("c"),
          c.getString("expected")
        )
      }
  }

  // -------- AWS (optional) --------
  object aws {
    val s3Bucket: String    = cfg.getString("aws.s3Bucket")
    val s3IndexPref: String = cfg.getString("aws.s3IndexPref")
  }

  // -------- Export config values into Hadoop Configuration --------
  object hadoopKeys {
    val ChunkWindow  = "app.chunk.windowChars"
    val ChunkOverlap = "app.chunk.overlapChars"
    val MrTopK       = "app.mr.topK"
    val VecDim       = "app.models.vectorDim"
    val SimKind      = "app.models.similarity"
  }

  import org.apache.hadoop.conf.Configuration
  def writeTo(conf: Configuration): Unit = {
    conf.setInt(hadoopKeys.ChunkWindow,  chunking.windowChars)
    conf.setInt(hadoopKeys.ChunkOverlap, chunking.overlapChars)
    conf.setInt(hadoopKeys.MrTopK,       mr.topK)
    conf.setInt(hadoopKeys.VecDim,       models.vectorDim)
    conf.set(hadoopKeys.SimKind,         models.similarity)
  }
}
