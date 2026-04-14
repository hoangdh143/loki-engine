package loki.engine.backend

import zio.*
import scala.util.Try
import scala.collection.mutable

case class ToolCall(
  id: String,
  name: String,
  input: Map[String, Any]
)

case class ToolResult(
  toolCallId: String,
  content: String
)

case class LLMResponse(
  text: String,
  toolCalls: List[ToolCall],
  // "end_turn" | "tool_use" | "max_tokens" | "unparsed_tool_call"
  //
  // "unparsed_tool_call" means the model wrote a tool call the API layer never
  // turned into a structured call and that could not be recovered from the text
  // either — the turn did no work and is NOT a final answer. See ToolCallRecovery.
  stopReason: String,
  raw: Any = null,
  reasoning: String = ""
)

trait LLMBackend {
  def chat(
    model: String,
    system: String,
    messages: List[Map[String, Any]],
    tools: List[Map[String, Any]],
    maxTokens: Int = 8096
  ): Task[LLMResponse]

  def extendMessages(
    messages: List[Map[String, Any]],
    response: LLMResponse,
    toolResults: List[ToolResult]
  ): List[Map[String, Any]]
}

trait Embedder {
  def embedText(text: String): Task[List[Double]]
}
