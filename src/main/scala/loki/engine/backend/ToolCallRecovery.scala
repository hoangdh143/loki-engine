package loki.engine.backend

import loki.engine.core.executor.JsonUtil
import scala.collection.mutable
import scala.util.control.NonFatal

// ---------------------------------------------------------------------------
// Unparsed tool calls (local models)
// ---------------------------------------------------------------------------
// A local model sometimes emits a tool call as literal TEXT instead of through the
// API's structured `tool_calls` field — the server's parser rejected its output (a
// format it doesn't recognise, or a hybrid of two conventions). The turn then looks
// like a plain final answer, so an agent loop ends mid-task believing the model was
// finished. Detecting it lets the executor ask for a properly-formed call instead of
// silently terminating.
//
// The markup lands in EITHER channel, so both must be inspected: a reasoning model
// often returns an empty/whitespace `content` and puts everything — including the
// tool call it meant to make — in `reasoning_content`. Checking only `content` misses
// exactly the case that motivated this (a 27b local model emitting a Hermes/Llama
// hybrid into the reasoning channel, leaving `content` as "\n\n").
private[engine] object ToolCallRecovery {

  // Matching is deliberately conservative — an explicit opening tag from a known
  // convention. Prose that merely mentions tools must not trip it.
  private val UnparsedMarkers = (
    """(?ix)(""" +
    """   <\s*tool_call\s*>""" +        // Hermes / Qwen / many GGUF chat templates
    """ | <\s*function\s*=""" +         // Llama-3 / functionary: <function=name>
    """ | <\s*function_call\s*>""" +
    """ | <\|\s*tool_call\s*\|>""" +    // ChatML-style sentinel
    """ | <\s*invoke\s+name\s*=""" +    // Anthropic-XML imitation
    """)"""
  ).r

  /** Whether ANY response channel carries a tool-call block the API never parsed.
    *
    * Pass every channel the model can write into (`content` and `reasoning`) — the
    * markup shows up in either, and a reasoning model routinely leaves `content`
    * blank. Only meaningful for a turn that returned NO structured tool calls while
    * tools were on offer — see the callers, which check both. */
  def looksLikeUnparsedToolCall(channels: String*): Boolean =
    channels.exists(c => c != null && c.nonEmpty && UnparsedMarkers.findFirstIn(c).isDefined)

  // --- Recovering the call, not just detecting it ----------------------------
  // Re-prompting costs a turn and relies on the model complying (a small local model
  // often does not). When the markup is present we can usually just READ it: the name
  // and arguments are right there. Two dialects cover what local models emit, plus the
  // hybrid of both:
  //
  //   Hermes / Qwen:  <tool_call>{"name": "f", "arguments": {...}}</tool_call>
  //   Llama-3:        <function=f><parameter=k>v</parameter></function>
  //   hybrid:         <tool_call><function=f><parameter=k>\nv\n</parameter></function></tool_call>
  private val FuncNameRe = """(?i)<\s*function\s*=\s*([A-Za-z0-9_.\-]+)\s*>""".r
  private val ParamRe =
    """(?is)<\s*parameter\s*=\s*([A-Za-z0-9_.\-]+)\s*>(.*?)(?:<\s*/\s*parameter\s*>|(?=<\s*parameter\s*=)|\z)""".r
  private val HermesBlockRe = """(?is)<\s*tool_call\s*>(.*?)(?:<\s*/\s*tool_call\s*>|\z)""".r

  private def asObject(value: Any): Option[Map[String, Any]] = value match {
    case m: Map[?, ?] => Some(m.asInstanceOf[Map[String, Any]])
    case _            => None
  }

  private def parseJson(text: String): Option[Any] =
    try Some(JsonUtil.fromJson(text)) catch { case NonFatal(_) => None }

  /** Recover structured ToolCalls from tool-call markup the API layer left as text.
    *
    * Returns Nil when nothing parseable is present — the caller then falls back to
    * re-prompting. Never throws: a half-written block is simply not a call. */
  def parseTextToolCalls(channels: String*): List[ToolCall] = {
    val calls = mutable.ListBuffer[ToolCall]()
    channels.filter(c => c != null && c.nonEmpty).foreach { chan =>
      // Hermes-style JSON bodies first — a hybrid block also matches here, but its
      // body is not JSON, so it falls through to the Llama scan below.
      HermesBlockRe.findAllMatchIn(chan).foreach { block =>
        val body = block.group(1).trim
        if (body.startsWith("{")) {
          parseJson(body).flatMap(asObject).foreach { obj =>
            val name = obj.get("name").orElse(obj.get("function")).map(_.toString).filter(_.nonEmpty)
            val args: Map[String, Any] = obj.get("arguments") match {
              case Some(m: Map[?, ?]) => m.asInstanceOf[Map[String, Any]]
              case Some(s: String)    => parseJson(s).flatMap(asObject).getOrElse(Map.empty)
              case _                  => Map.empty
            }
            name.foreach(n => calls += ToolCall(s"recovered_${calls.length}", n, args))
          }
        }
      }
      // Llama-3 / hybrid: <function=name> with <parameter=key>value</parameter>
      val functions = FuncNameRe.findAllMatchIn(chan).toList
      functions.zipWithIndex.foreach { case (fn, index) =>
        val name = fn.group(1)
        // Scan only up to the next function block so params aren't cross-assigned.
        val segmentEnd = functions.lift(index + 1).map(_.start).getOrElse(chan.length)
        val segment = chan.substring(fn.end, segmentEnd)
        val args: Map[String, Any] =
          ParamRe.findAllMatchIn(segment).map(p => p.group(1) -> (p.group(2).trim: Any)).toMap
        // Already recovered from the JSON pass above.
        if (!calls.exists(c => c.name == name && c.input == args))
          calls += ToolCall(s"recovered_${calls.length}", name, args)
      }
    }
    calls.toList
  }

  // --- Detecting arguments the SERVER mangled --------------------------------
  // LM Studio sometimes half-parses an XML call into a real `tool_calls` entry whose
  // ARGUMENT KEYS are markup fragments, e.g. observed verbatim:
  //   {"actions\n</parameter": "<parameter=limit>\n5", "patterns\n</parameter": "..."}
  // The call then "succeeds" with every real argument lost — there it ran an unfiltered
  // query and the model silently got the wrong answer. A wrong answer is worse than a
  // failed call, so treat this as an unparsed call and ask for a clean one.
  //
  // Values are inspected as well as keys, which a plain JSON decode does not
  // need to do: a JSON decode preserves a mangled key verbatim, whereas this
  // backend's argument scanner only admits `\w+` keys and DROPS a markup-laden one.
  // The mangled VALUE is what survives here, so keys alone would never fire.
  private val MangledMarkers = List("</parameter", "<parameter=", "</function", "<function=")

  private def isMangled(text: String): Boolean = {
    val low = text.toLowerCase
    MangledMarkers.exists(low.contains)
  }

  /** True when any call's arguments carry tool-call markup — a server-side mis-parse,
    * not arguments the model meant to send. */
  def hasMangledToolArgs(toolCalls: List[ToolCall]): Boolean =
    toolCalls.exists { call =>
      call.input != null && call.input.exists { case (key, value) =>
        isMangled(String.valueOf(key)) || (value match {
          case s: String => isMangled(s)
          case _         => false
        })
      }
    }
}
