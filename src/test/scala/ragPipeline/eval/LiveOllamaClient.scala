package ragPipeline.PDFsToShardMapReduce

import io.circe._
import io.circe.generic.semiauto._
import sttp.client3._
import sttp.client3.circe._
import scala.concurrent.duration._

class LiveOllamaClient(base: String = sys.env.getOrElse("OLLAMA_HOST","http://127.0.0.1:11434")){
  private val backend = HttpClientSyncBackend()
  private val embedUrl = uri"$base/api/embed"
  private val chatUrl  = uri"$base/api/chat"

  import ragPipeline.models.OllamaJson._

  /** Return L2-normalized embeddings as arrays of Float. */
  def embed(
             texts: Vector[String],
             model: String = "mxbai-embed-large"
           ): Vector[Array[Float]] = {
    val req = basicRequest
      .readTimeout(300.seconds) // allow cold-start
      .post(embedUrl)
      .body(EmbedReq(model, texts, Some("15m")))
      .response(asJson[EmbedResp])

    req.send(backend).body.fold(throw _, r => r.embeddings.map(_.toArray))
  }

  /** Simple chat call; returns message content. */
  def chat(
            messages: Vector[ChatMessage],
            model: String = "llama3.1:8b"
          ): String = {
    val req = basicRequest
      .readTimeout(300.seconds)
      .post(chatUrl)
      .body(ChatReq(model, messages, stream = false))
      .response(asJson[ChatResp])

    req.send(backend).body.fold(throw _, r => r.message.content)
  }
}

object OllamaJson {
  final case class EmbedReq(model: String, input: Vector[String], keep_alive: Option[String] = None)
  final case class EmbedResp(embeddings: Vector[Vector[Float]])

  final case class ChatMessage(role: String, content: String)
  final case class ChatReq(model: String, messages: Vector[ChatMessage], stream: Boolean = false)
  final case class ChatMsg(role: String, content: String)
  final case class ChatResp(message: ChatMsg)

  // Circe codecs (Scala 2 style)
  implicit val encEmbedReq: Encoder[EmbedReq] = deriveEncoder[EmbedReq]
  implicit val decEmbedResp: Decoder[EmbedResp] =
    Decoder.instance { c =>
      // Try plural form first, then fall back to singular
      c.downField("embeddings").as[Vector[Vector[Float]]] match {
        case Right(vv) => Right(EmbedResp(vv))
        case Left(_) =>
          c.downField("embedding").as[Vector[Float]] match {
            case Right(v)  => Right(EmbedResp(Vector(v)))
            case Left(err) => Left(err)
          }
      }
    }

  implicit val encChatMessage: Encoder[ChatMessage] = deriveEncoder[ChatMessage]
  implicit val encChatReq: Encoder[ChatReq]         = deriveEncoder[ChatReq]
  implicit val decChatMsg:  Decoder[ChatMsg]        = deriveDecoder[ChatMsg]
  implicit val decChatResp: Decoder[ChatResp]       = deriveDecoder[ChatResp]
}