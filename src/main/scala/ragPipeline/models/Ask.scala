package ragPipeline.models

import ragPipeline.config.AppConfig
import ragPipeline.models.OllamaJson.ChatMessage

import scala.util.Try

object Ask {

//  // crude but effective size guard; adjust as needed
//  private val MaxQuestionChars = 300
//  private val MaxContextChars  = 1500


  // Use config instead (fallbacks shown)
  private val MaxQuestionChars: Int = 300
  private val MaxContextChars:  Int =
    Try(AppConfig.chunking.maxContextBlock).getOrElse(2048)  // match your application.conf

  /** Build robust messages: system guardrail + one user turn with a trimmed context. */
  def buildMessages(question: String, context: String): Vector[ChatMessage] = {
    val q = question.take(MaxQuestionChars)
//    val q = question
    val c = context.take(MaxContextChars)

    val system = ChatMessage(
      role   = "system",
      content =
        "You are a concise TA. Answer ONLY from the provided context. " +
          "If the answer isn’t present, reply exactly: I can’t find that in the context."
    )

    val user = ChatMessage(
      role    = "user",
      content =
        s"""Question: $q

Use ONLY this context (truncated):
$c"""
    )

    Vector(system, user)
  }

  /** Send the chat using a proper chat model (NOT the embedding model). */
  def ask(client: Ollama, question: String, context: String, model: String = AppConfig.models.chatModel): String = {
    val msgs = buildMessages(question, context)
    // Let the client enforce short deterministic generations
    client.chat(messages = msgs, model = model)
  }
}
