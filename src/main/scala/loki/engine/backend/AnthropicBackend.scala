package loki.engine.backend

import zio.*
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import scala.collection.mutable

/**
 * LLMBackend implementation for Anthropic's Messages API.
 *
 * Reads the API key from the ANTHROPIC_API_KEY environment variable.
 * Sends requests to https://api.anthropic.com/v1/messages using the
 * native Java 11 HttpClient (no additional dependencies required).
 */
class AnthropicBackend(
  apiKey: String = sys.env.getOrElse("ANTHROPIC_API_KEY", ""),
  baseUrl: String = "https://api.anthropic.com/v1/messages",
  anthropicVersion: String = "2023-06-01"
) extends LLMBackend {

  private val httpClient = HttpClient.newHttpClient()

  def chat(
    model: String,
    system: String,
    messages: List[Map[String, Any]],
    tools: List[Map[String, Any]],
    maxTokens: Int = 8096
  ): Task[LLMResponse] = ZIO.attemptBlocking {
    val body = buildRequestBody(model, system, messages, tools, maxTokens)
    val request = HttpRequest.newBuilder()
      .uri(URI.create(baseUrl))
      .header("Content-Type", "application/json")
      .header("x-api-key", apiKey)
      .header("anthropic-version", anthropicVersion)
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200) {
      throw new RuntimeException(s"Anthropic API error ${response.statusCode()}: ${response.body().take(500)}")
    }
    parseResponse(response.body())
  }

  def extendMessages(
    messages: List[Map[String, Any]],
    response: LLMResponse,
    toolResults: List[ToolResult]
  ): List[Map[String, Any]] = {
    val assistantContent: List[Map[String, Any]] =
      (if (response.text.nonEmpty) List(Map[String, Any]("type" -> "text", "text" -> response.text)) else Nil) ++
      response.toolCalls.map(tc => Map[String, Any](
        "type" -> "tool_use",
        "id" -> tc.id,
        "name" -> tc.name,
        "input" -> tc.input
      ))

    val toolResultContent: List[Map[String, Any]] = toolResults.map(tr => Map[String, Any](
      "type" -> "tool_result",
      "tool_use_id" -> tr.toolCallId,
      "content" -> tr.content
    ))

    messages ++
      List(Map[String, Any]("role" -> "assistant", "content" -> assistantContent)) ++
      List(Map[String, Any]("role" -> "user", "content" -> toolResultContent))
  }

  // ---- JSON building -------------------------------------------------------

  private def buildRequestBody(
    model: String,
    system: String,
    messages: List[Map[String, Any]],
    tools: List[Map[String, Any]],
    maxTokens: Int
  ): String = {
    val sb = new StringBuilder
    sb.append(s"""{"model":${jsonStr(model)},"max_tokens":$maxTokens""")
    if (system.nonEmpty) sb.append(s""","system":${jsonStr(system)}""")
    sb.append(s""","messages":${jsonMessages(messages)}""")
    if (tools.nonEmpty) sb.append(s""","tools":${jsonTools(tools)}""")
    sb.append("}")
    sb.toString()
  }

  private def jsonMessages(messages: List[Map[String, Any]]): String = {
    val items = messages.map { m =>
      val role = m.get("role").map(_.toString).getOrElse("user")
      val content = m.get("content") match {
        case Some(s: String) => jsonStr(s)
        case Some(l: List[_]) => jsonContentBlocks(l.asInstanceOf[List[Map[String, Any]]])
        case _ => jsonStr("")
      }
      s"""{"role":${jsonStr(role)},"content":$content}"""
    }
    s"[${items.mkString(",")}]"
  }

  private def jsonContentBlocks(blocks: List[Map[String, Any]]): String = {
    val items = blocks.map { b =>
      b.get("type").map(_.toString) match {
        case Some("text") =>
          s"""{"type":"text","text":${jsonStr(b.get("text").map(_.toString).getOrElse(""))}}"""
        case Some("tool_use") =>
          val id = b.get("id").map(_.toString).getOrElse("")
          val name = b.get("name").map(_.toString).getOrElse("")
          val input = b.get("input").map(anyToJson).getOrElse("{}")
          s"""{"type":"tool_use","id":${jsonStr(id)},"name":${jsonStr(name)},"input":$input}"""
        case Some("tool_result") =>
          val toolUseId = b.get("tool_use_id").map(_.toString).getOrElse("")
          val content = b.get("content").map(_.toString).getOrElse("")
          s"""{"type":"tool_result","tool_use_id":${jsonStr(toolUseId)},"content":${jsonStr(content)}}"""
        case _ =>
          s"""{"type":"text","text":${jsonStr(b.toString)}}"""
      }
    }
    s"[${items.mkString(",")}]"
  }

  private def jsonTools(tools: List[Map[String, Any]]): String = {
    val items = tools.map { t =>
      val name = t.get("name").map(_.toString).getOrElse("")
      val desc = t.get("description").map(_.toString).getOrElse("")
      val schema = t.get("input_schema").map(anyToJson).getOrElse("""{"type":"object","properties":{}}""")
      s"""{"name":${jsonStr(name)},"description":${jsonStr(desc)},"input_schema":$schema}"""
    }
    s"[${items.mkString(",")}]"
  }

  private def anyToJson(v: Any): String = v match {
    case null => "null"
    case s: String => jsonStr(s)
    case b: Boolean => b.toString
    case n: Number => n.toString
    case m: Map[_, _] =>
      val pairs = m.asInstanceOf[Map[String, Any]].map { case (k, vv) => s"${jsonStr(k)}:${anyToJson(vv)}" }
      s"{${pairs.mkString(",")}}"
    case l: List[_] =>
      s"[${l.map(anyToJson).mkString(",")}]"
    case other => jsonStr(other.toString)
  }

  private def jsonStr(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\""

  // ---- JSON parsing --------------------------------------------------------

  private[engine] def parseResponse(body: String): LLMResponse = {
    val stopReason = extractStr(body, "stop_reason").getOrElse("end_turn")
    val text = extractAllTextBlocks(body)
    val reasoning = extractAllThinkingBlocks(body)
    val toolCalls = extractToolCalls(body)
    LLMResponse(text = text, toolCalls = toolCalls, stopReason = stopReason, raw = body, reasoning = reasoning)
  }

  private def extractStr(json: String, key: String): Option[String] = {
    val pat = s""""$key"\\s*:\\s*"([^"]*?)"""".r
    pat.findFirstMatchIn(json).map(_.group(1))
  }

  private def extractAllTextBlocks(body: String): String = {
    val pat = """"type"\s*:\s*"text"\s*,\s*"text"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
    pat.findAllMatchIn(body).map(_.group(1)
      .replace("\\n", "\n")
      .replace("\\\"", "\"")
      .replace("\\\\", "\\")
    ).mkString("")
  }

  private def extractAllThinkingBlocks(body: String): String = {
    val pat = """"type"\s*:\s*"thinking"\s*,\s*"thinking"\s*:\s*"((?:[^"\\]|\\.)*)""".r
    pat.findAllMatchIn(body).map(_.group(1)
      .replace("\\n", "\n")
      .replace("\\\"", "\"")
      .replace("\\\\", "\\")
    ).mkString("")
  }

  private def extractToolCalls(body: String): List[ToolCall] = {
    val toolUsePattern = """"type"\s*:\s*"tool_use"[^}]*?"id"\s*:\s*"([^"]+)"[^}]*?"name"\s*:\s*"([^"]+)"""".r
    val calls = mutable.ListBuffer[ToolCall]()
    // Find all tool_use blocks and extract id + name, then locate their input
    val objPattern = """\{[^{}]*?"type"\s*:\s*"tool_use"[^{}]*?\}""".r
    // Since tool_use blocks have nested "input" objects, use a simpler bracket-aware scan
    extractToolUseBlocks(body).foreach { block =>
      val id = extractStr(block, "id").getOrElse("")
      val name = extractStr(block, "name").getOrElse("")
      val input = extractInputObject(block)
      if (name.nonEmpty) calls += ToolCall(id, name, input)
    }
    calls.toList
  }

  private def extractToolUseBlocks(body: String): List[String] =
    JsonScan.findBlocks(body, "\"type\":\"tool_use\"", "\"type\": \"tool_use\"")

  private def extractInputObject(block: String): Map[String, Any] = {
    val inputIdx = block.indexOf("\"input\"")
    val braceStart = if (inputIdx < 0) -1 else block.indexOf('{', inputIdx)
    val objEnd = if (braceStart < 0) -1 else JsonScan.balancedEnd(block, braceStart)
    if (objEnd < 0) Map.empty
    else parseSimpleObject(block.substring(braceStart, objEnd + 1))
  }

  private def parseSimpleObject(json: String): Map[String, Any] = {
    val result = mutable.Map[String, Any]()
    val strPat = """"(\w+)"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
    strPat.findAllMatchIn(json).foreach(m => result.put(m.group(1), m.group(2).replace("\\n", "\n").replace("\\\"", "\"")))
    val numPat = """"(\w+)"\s*:\s*([\d.]+)""".r
    numPat.findAllMatchIn(json).foreach(m => if (!result.contains(m.group(1))) result.put(m.group(1), m.group(2).toDoubleOption.getOrElse(0.0)))
    val boolPat = """"(\w+)"\s*:\s*(true|false)""".r
    boolPat.findAllMatchIn(json).foreach(m => if (!result.contains(m.group(1))) result.put(m.group(1), m.group(2) == "true"))
    result.toMap
  }
}
