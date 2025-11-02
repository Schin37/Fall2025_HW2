package ragPipeline.models

import io.circe._
import io.circe.generic.semiauto._
import ragPipeline.config.AppConfig
import sttp.client3._
import sttp.client3.circe._
import sttp.model.MediaType
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

/** Minimal synchronous client for a local Ollama server with configurable timeouts and retries. */
class Ollama(base: String = Ollama.detectBaseUrl()) {

  import OllamaJson._

  private val cfg = ConfigFactory.load().getConfig("app.models.ollama")
  private val modelName = if (cfg.hasPath("model")) cfg.getString("model") else AppConfig.models.chatModel
  private val connectMs = if (cfg.hasPath("connectMs")) cfg.getInt("connectMs") else 30000
  private val readMs    = if (cfg.hasPath("readMs"))    cfg.getInt("readMs")    else 60000 // 5 min
  private val attempts  = if (cfg.hasPath("attempts"))  cfg.getInt("attempts")  else 3

  private val backend: SttpBackend[Identity, Any] =
    HttpClientSyncBackend(
      options = SttpBackendOptions.connectionTimeout(connectMs.millis)
    )

  // then on each request:
  basicRequest.readTimeout(readMs.millis)

  private val embedUrl = uri"$base/api/embed"
  private val chatUrl  = uri"$base/api/chat"

  /** Return L2-normalized embeddings as arrays of Float. */
  def embed(
             texts: Vector[String],
             model: String = AppConfig.models.embedModel,
             keepAlive: Option[String] = Some("15m")
           ): Vector[Array[Float]] = {
    val req = basicRequest
      .post(embedUrl)
      .readTimeout(readMs.millis)
      .contentType(MediaType.ApplicationJson)
      .body(EmbedReq(model, texts, keepAlive))
      .response(asJson[EmbedResp])
    sendWithRetry(req, attempts).embeddings.map(_.toArray)
  }



  /** Simple chat; returns assistant content. */
  def chat(
            messages: Vector[ChatMessage],
            model: String = modelName,
            options: ChatOptions = ChatOptions.default.copy(
              num_ctx = Some(if (cfg.hasPath("num_ctx")) cfg.getInt("num_ctx") else 8000),
              temperature = Some(if (cfg.hasPath("temperature")) cfg.getDouble("temperature") else 0.2)
            )
          ): String = {
    val req = basicRequest
      .post(chatUrl)
      .readTimeout(readMs.millis)
      .contentType(MediaType.ApplicationJson)
      .body(ChatReq(model, messages, stream = false, options = options, keep_alive = Some("10m")))
      .response(asJson[ChatResp])
    sendWithRetry(req, attempts).message.content
  }

  /** Retry helper with simple exponential backoff. */
  private def sendWithRetry[A](req: Request[Either[ResponseException[String, Error], A], Any], attempts: Int): A = {
    var i = 0
    var last: Throwable = null
    while (i < attempts) {
      try {
        val resp = req.send(backend)
        return resp.body.fold(throw _, identity)
      } catch {
        case t: java.net.http.HttpTimeoutException =>
          println(s"[WARN] Ollama request timed out (attempt ${i+1}/$attempts, waiting 5s): ${t.getMessage}")
          last = t
        case t: Throwable =>
          println(s"[WARN] Ollama send failed (attempt ${i+1}/$attempts): ${t.getMessage}")
          last = t
      }
      Thread.sleep(5000L * (i + 1))
      i += 1
    }
    throw new RuntimeException(s"Ollama request failed after $attempts attempts: ${last.getMessage}", last)
  }
}

object Ollama {
  /** Prefer OLLAMA_BASE_URL; fall back to OLLAMA_HOST; else localhost default. */
  def detectBaseUrl(): String =
    sys.env.get("OLLAMA_BASE_URL")
      .orElse(sys.env.get("OLLAMA_HOST"))
      .getOrElse("http://127.0.0.1:11434")
}

object OllamaJson {
  // --------- Embeddings ----------
  final case class EmbedReq(model: String, input: Vector[String], keep_alive: Option[String] = None)
  final case class EmbedResp(embeddings: Vector[Vector[Float]])

  // --------- Chat ----------
  final case class ChatMessage(role: String, content: String)
  final case class ChatOptions(
                                num_predict: Int = 64,
                                temperature: Option[Double] = None,
                                stop: Option[Vector[String]] = None,
                                num_ctx: Option[Int] = None
                              )
  object ChatOptions { val default: ChatOptions = ChatOptions() }

  final case class ChatReq(
                            model: String,
                            messages: Vector[ChatMessage],
                            stream: Boolean = false,
                            options: ChatOptions = ChatOptions.default,
                            keep_alive: Option[String] = Some("10m")
                          )
  final case class ChatMsg(role: String, content: String)
  final case class ChatResp(message: ChatMsg)

  // Circe codecs
  implicit val encEmbedReq:  Encoder[EmbedReq]   = deriveEncoder
  implicit val decEmbedResp: Decoder[EmbedResp]  = deriveDecoder
  implicit val encChatMessage: Encoder[ChatMessage] = deriveEncoder
  implicit val encChatOptions: Encoder[ChatOptions] = deriveEncoder
  implicit val encChatReq:     Encoder[ChatReq]     = deriveEncoder
  implicit val decChatMsg:     Decoder[ChatMsg]     = deriveDecoder
  implicit val decChatResp:    Decoder[ChatResp]    = deriveDecoder
}
