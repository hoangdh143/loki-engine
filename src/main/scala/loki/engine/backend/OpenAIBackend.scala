package loki.engine.backend

import zio.*
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import scala.collection.mutable

/**
 * LLMBackend for any OpenAI-compatible endpoint: Ollama, mlx-lm, LM Studio,
 * vLLM, OpenAI, Azure OpenAI, etc.
 *
 * Reads the API key from OPENAI_API_KEY (optional — omit for local models).
 */
class OpenAIBackend(
  baseUrl: String = sys.env.getOrElse("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions"),
  apiKey: String = sys.env.getOrElse("OPENAI_API_KEY", ""),
  // When set, sent on every completion. Local tool-calling models emit
  // well-formed tool calls far more reliably at a low temperature; left None
  // it preserves the server/default sampling.
  temperature: Option[Double] = None
) extends LLMBackend {

  private val httpClient = HttpClient.newHttpClient()

  def chat(
    model: String,
    system: String,
    messages: List[Map[String, Any]],
    tools: List[Map[String, Any]],
    maxTokens: Int = 8096
  ): Task[LLMResponse] = ZIO.attemptBlocking {
    val oaiMessages = convertMessages(system, messages)
    val body = buildRequestBody(model, oaiMessages, tools, maxTokens)

    val reqBuilder = HttpRequest.newBuilder()
      .uri(URI.create(baseUrl))
      .header("Content-Type", "application/json")
    if (apiKey.nonEmpty) reqBuilder.header("Authorization", s"Bearer $apiKey")
    val request = reqBuilder.POST(HttpRequest.BodyPublishers.ofString(body)).build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200) {
      throw new RuntimeException(s"OpenAI-compatible API error ${response.statusCode()}: ${response.body().take(500)}")
    }
    parseResponse(response.body(), toolsOffered = tools.nonEmpty)
  }

  def extendMessages(
    messages: List[Map[String, Any]],
    response: LLMResponse,
    toolResults: List[ToolResult]
  ): List[Map[String, Any]] = {
    val assistantMsg: Map[String, Any] = if (response.toolCalls.nonEmpty) {
      val toolCallsJson = response.toolCalls.map(tc =>
        Map[String, Any](
          "id" -> tc.id,
          "type" -> "function",
          "function" -> Map[String, Any]("name" -> tc.name, "arguments" -> mapToJsonStr(tc.input))
        )
      )
      Map[String, Any]("role" -> "assistant", "content" -> response.text, "tool_calls" -> toolCallsJson)
    } else {
      Map[String, Any]("role" -> "assistant", "content" -> response.text)
    }

    val toolResultMsgs: List[Map[String, Any]] = toolResults.map(tr =>
      Map[String, Any]("role" -> "tool", "tool_call_id" -> tr.toolCallId, "content" -> tr.content)
    )

    messages ++ List(assistantMsg) ++ toolResultMsgs
  }

  // ---- OpenAI message format -----------------------------------------------

  private def convertMessages(system: String, messages: List[Map[String, Any]]): List[Map[String, Any]] = {
    val systemMsg = if (system.nonEmpty) List(Map[String, Any]("role" -> "system", "content" -> system)) else Nil
    val converted = messages.map { m =>
      val role = m.get("role").map(_.toString).getOrElse("user")
      m.get("content") match {
        case Some(l: List[_]) =>
          // Flatten content blocks to a plain string for OpenAI
          val text = l.asInstanceOf[List[Map[String, Any]]].collect {
            case b if b.get("type").contains("text") => b.get("text").map(_.toString).getOrElse("")
          }.mkString(" ")
          Map[String, Any]("role" -> role, "content" -> text)
        case _ => m
      }
    }
    val combined = systemMsg ++ converted
    // Qwen3 requires the first non-system message to be a user message. After
    // tool calls the history is [assistant, tool, ...] with no leading user
    // turn, so inject a minimal one right after the system message when needed.
    val systemCount = combined.count(_.get("role").map(_.toString).contains("system"))
    val firstNonSystemRole = combined.drop(systemCount).headOption.flatMap(_.get("role").map(_.toString))
    if (!firstNonSystemRole.contains("user")) {
      val (head, tail) = combined.splitAt(systemCount)
      head ++ List(Map[String, Any]("role" -> "user", "content" -> "Begin.")) ++ tail
    } else combined
  }

  private def buildRequestBody(
    model: String,
    messages: List[Map[String, Any]],
    tools: List[Map[String, Any]],
    maxTokens: Int
  ): String = {
    val sb = new StringBuilder
    sb.append(s"""{"model":${jStr(model)},"max_tokens":$maxTokens""")
    temperature.foreach(t => sb.append(s""","temperature":$t"""))
    sb.append(s""","messages":${jsonMessages(messages)}""")
    if (tools.nonEmpty) sb.append(s""","tools":${jsonTools(tools)}""")
    sb.append("}")
    sb.toString()
  }

  private def jsonMessages(messages: List[Map[String, Any]]): String = {
    val items = messages.map { m =>
      val role = m.get("role").map(_.toString).getOrElse("user")
      val content = m.get("content") match {
        case Some(s: String) => jStr(s)
        case _ => jStr("")
      }
      val base = s"""{"role":${jStr(role)},"content":$content"""
      val toolCallsPart = m.get("tool_calls") match {
        case Some(l: List[_]) =>
          val calls = l.asInstanceOf[List[Map[String, Any]]].map { tc =>
            val id = tc.get("id").map(_.toString).getOrElse("")
            val fn = tc.get("function").map(_.asInstanceOf[Map[String, Any]]).getOrElse(Map.empty)
            val name = fn.get("name").map(_.toString).getOrElse("")
            val args = fn.get("arguments").map(_.toString).getOrElse("{}")
            s"""{"id":${jStr(id)},"type":"function","function":{"name":${jStr(name)},"arguments":${jStr(args)}}}"""
          }
          s""","tool_calls":[${calls.mkString(",")}]"""
        case _ => ""
      }
      val toolCallIdPart = m.get("tool_call_id").map(id => s""","tool_call_id":${jStr(id.toString)}""").getOrElse("")
      base + toolCallsPart + toolCallIdPart + "}"
    }
    s"[${items.mkString(",")}]"
  }

  private def jsonTools(tools: List[Map[String, Any]]): String = {
    val items = tools.map { t =>
      val name = t.get("name").map(_.toString).getOrElse("")
      val desc = t.get("description").map(_.toString).getOrElse("")
      val schema = t.get("input_schema").map(anyToJson).getOrElse("""{"type":"object","properties":{}}""")
      s"""{"type":"function","function":{"name":${jStr(name)},"description":${jStr(desc)},"parameters":$schema}}"""
    }
    s"[${items.mkString(",")}]"
  }

  private def mapToJsonStr(m: Map[String, Any]): String = anyToJson(m)

  private def anyToJson(v: Any): String = v match {
    case null => "null"
    case s: String => jStr(s)
    case b: Boolean => b.toString
    case n: Number => n.toString
    case m: Map[_, _] =>
      val pairs = m.asInstanceOf[Map[String, Any]].map { case (k, vv) => s"${jStr(k)}:${anyToJson(vv)}" }
      s"{${pairs.mkString(",")}}"
    case l: List[_] => s"[${l.map(anyToJson).mkString(",")}]"
    case other => jStr(other.toString)
  }

  private def jStr(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\""

  // ---- Response parsing ----------------------------------------------------

  // `toolsOffered` gates the unparsed-tool-call recovery below: a turn that was never
  // given tools cannot have meant to call one, and prose about `<function=…>` must not
  // be rewritten into a call. Defaults to false so a direct parse stays literal.
  private[engine] def parseResponse(body: String, toolsOffered: Boolean = false): LLMResponse = {
    val finishReason = extractStr(body, "finish_reason").getOrElse("stop")
    val baseStopReason = finishReason match {
      case "tool_calls" => "tool_use"
      case "length"     => "max_tokens"
      case _            => "end_turn"
    }
    val text = extractStr(body, "content").getOrElse("")
    val reasoning = extractStr(body, "reasoning_content").getOrElse("")
    val parsedCalls = extractToolCalls(body)

    // The server half-parsed an XML call into `tool_calls` whose arguments are markup
    // fragments — every real argument lost. Running that is worse than failing it (the
    // tool silently answers a different question), so drop the mangled calls and treat
    // the turn as an unparsed call.
    val (mangleFixedCalls, mangleFixedStop) =
      if (toolsOffered && parsedCalls.nonEmpty && ToolCallRecovery.hasMangledToolArgs(parsedCalls)) {
        val recovered = ToolCallRecovery.parseTextToolCalls(text, reasoning)
        (recovered, if (recovered.nonEmpty) "tool_use" else "unparsed_tool_call")
      } else (parsedCalls, baseStopReason)

    // The model wrote a tool call as text and the server didn't parse it into
    // `tool_calls`. That is NOT the end of the turn. Prefer RECOVERING it — the name
    // and arguments are right there, and re-prompting both costs a turn and relies on
    // a model that often cannot comply. Only flag for a re-prompt when the markup is
    // present but unparseable.
    val (toolCalls, stopReason) =
      if (
        mangleFixedStop == "end_turn" && mangleFixedCalls.isEmpty && toolsOffered &&
        ToolCallRecovery.looksLikeUnparsedToolCall(text, reasoning)
      ) {
        val recovered = ToolCallRecovery.parseTextToolCalls(text, reasoning)
        if (recovered.nonEmpty) (recovered, "tool_use") else (Nil, "unparsed_tool_call")
      } else (mangleFixedCalls, mangleFixedStop)

    LLMResponse(text = text, toolCalls = toolCalls, stopReason = stopReason, raw = body, reasoning = reasoning)
  }

  private def extractStr(json: String, key: String): Option[String] =
    JsonScan.extractStr(json, key)

  private def extractToolCalls(body: String): List[ToolCall] =
    // OpenAI format: {"id":"call_xxx","type":"function","function":{"name":"...","arguments":"..."}}
    JsonScan.findBlocks(body, "\"type\":\"function\"", "\"type\": \"function\"").flatMap { block =>
      val id = JsonScan.extractStr(block, "id").getOrElse(java.util.UUID.randomUUID().toString)
      // The function sub-object
      val fnStart = block.indexOf("\"function\"")
      val fnContent = if (fnStart >= 0) block.substring(fnStart) else block
      val name = JsonScan.extractStr(fnContent, "name").getOrElse("")
      val argsStr = JsonScan.extractStr(fnContent, "arguments").getOrElse("{}")
      val input = parseSimpleObject(argsStr)
      if (name.nonEmpty) Some(ToolCall(id, name, input)) else None
    }

  private def parseSimpleObject(json: String): Map[String, Any] = {
    val result = mutable.Map[String, Any]()
    JsonScan.stringFields(json).foreach { case (key, value) => result.put(key, value) }
    val numPat = """"(\w+)"\s*:\s*([\d.]+)""".r
    numPat.findAllMatchIn(json).foreach(m => if (!result.contains(m.group(1))) result.put(m.group(1), m.group(2).toDoubleOption.getOrElse(0.0)))
    val boolPat = """"(\w+)"\s*:\s*(true|false)""".r
    boolPat.findAllMatchIn(json).foreach(m => if (!result.contains(m.group(1))) result.put(m.group(1), m.group(2) == "true"))
    result.toMap
  }
}
