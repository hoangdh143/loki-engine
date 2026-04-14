package loki.engine.viz

import zio.*
import zio.http.*
import loki.engine.core.executor.*
import loki.engine.core.nodes.EventType
import loki.engine.observability.*

/**
 * Browser-based trace visualizer backed by ZIO HTTP.
 *
 * Usage:
 *   TraceServer.serve(ctx)           // opens browser and blocks until Ctrl-C
 *
 * The server exposes:
 *   GET /         → interactive HTML trace tree
 *   GET /data     → JSON dump of the full trace
 *   GET /explain  → RunExplanation as JSON
 */
object TraceServer {

  def serve(ctx: ExecutionContext, port: Int = 8765, autoOpen: Boolean = true): Task[Nothing] =
    for {
      _ <- ZIO.when(autoOpen)(openBrowser(s"http://localhost:$port"))
      result <- Server.serve(routes(ctx)).provide(Server.defaultWithPort(port))
    } yield result

  private def routes(ctx: ExecutionContext): Routes[Any, Response] =
    Routes(
      Method.GET / "" -> handler(htmlResponse(ctx)),
      Method.GET / "data" -> handler(jsonDataResponse(ctx)),
      Method.GET / "explain" -> handler(explainResponse(ctx))
    )

  private def htmlResponse(ctx: ExecutionContext): Response = {
    val eventsJson = traceToJson(ctx)
    val html = buildHtml(ctx, eventsJson)
    Response.html(html)
  }

  private def jsonDataResponse(ctx: ExecutionContext): Response = {
    val body = traceToJson(ctx)
    Response(
      status = Status.Ok,
      headers = Headers(Header.ContentType(MediaType.application.json)),
      body = Body.fromString(body)
    )
  }

  private def explainResponse(ctx: ExecutionContext): Response = {
    val explanation = explainRun(ctx)
    val json = explanationToJson(explanation)
    Response(
      status = Status.Ok,
      headers = Headers(Header.ContentType(MediaType.application.json)),
      body = Body.fromString(json)
    )
  }

  private def traceToJson(ctx: ExecutionContext): String = {
    val events = ctx.trace.map { e =>
      val payload = e.payload.map { case (k, v) => s"${jStr(k)}:${anyToJson(v)}" }.mkString(",")
      s"""{
  "event_id":${jStr(e.eventId)},
  "event_type":${jStr(e.eventType.value)},
  "node_id":${jStr(e.nodeId)},
  "node_name":${jStr(e.nodeName)},
  "session_id":${jStr(e.sessionId)},
  "timestamp":${jStr(e.timestamp.toString)},
  "parent_event_id":${e.parentEventId.map(jStr).getOrElse("null")},
  "duration_ms":${e.durationMs.map(_.toString).getOrElse("null")},
  "payload":{$payload}
}"""
    }
    s"""{"session_id":${jStr(ctx.sessionId)},"query":${jStr(ctx.query.toString)},"hop_count":${ctx.hopCount},"events":[${events.mkString(",")}]}"""
  }

  private def explanationToJson(exp: RunExplanation): String = {
    val hops = exp.hops.map(h => s"""{"node_id":${jStr(h.nodeId)},"node_name":${jStr(h.nodeName)},"hop":${h.hop.getOrElse(0)},"summary":${jStr(h.summary)}}""")
    val routing = exp.routing.map(r => s"""{"node_id":${jStr(r.nodeId)},"intent":${r.intent.map(jStr).getOrElse("null")},"next_node_id":${r.nextNodeId.map(jStr).getOrElse("null")},"confidence":${r.confidence.getOrElse(0.0)},"mode":${jStr(r.mode)}}""")
    val tools = exp.toolCalls.map(t => s"""{"node_id":${jStr(t.nodeId)},"tool_name":${jStr(t.toolName)},"callable_ref":${jStr(t.callableRef)}}""")
    s"""{
  "session_id":${jStr(exp.sessionId)},
  "hop_count":${exp.hopCount},
  "paused":${exp.paused},
  "hops":[${hops.mkString(",")}],
  "routing":[${routing.mkString(",")}],
  "tool_calls":[${tools.mkString(",")}],
  "summary":{"routing_decision_count":${exp.summary.routingDecisionCount},"tool_call_count":${exp.summary.toolCallCount},"context_injection_count":${exp.summary.contextInjectionCount},"pause_event_count":${exp.summary.pauseEventCount}}
}"""
  }

  private def buildHtml(ctx: ExecutionContext, eventsJson: String): String = {
    val query = escapeHtml(ctx.query.toString)
    val sessionId = ctx.sessionId.take(8)
    s"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Loki Engine Trace — $sessionId</title>
$traceStyles
</head>
<body>
${traceHeader(query, sessionId, ctx)}
<div id="trace"></div>
<footer>Powered by Loki Engine</footer>
<script>
${traceScript(eventsJson)}
</script>
</body>
</html>"""
  }

  private def traceStyles: String =
    s"""<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'SF Mono', 'Cascadia Code', monospace; font-size: 13px; background: #0d1117; color: #c9d1d9; }
  header { padding: 16px 24px; background: #161b22; border-bottom: 1px solid #30363d; }
  header h1 { font-size: 16px; color: #f0f6fc; }
  header p { color: #8b949e; margin-top: 4px; }
  #trace { padding: 16px 24px; }
  .event { margin: 4px 0; border-left: 2px solid #30363d; padding-left: 12px; }
  .event.hop { border-color: #388bfd; }
  .event.agent_start { border-color: #3fb950; }
  .event.agent_end { border-color: #3fb950; }
  .event.tool_call { border-color: #d29922; }
  .event.tool_result { border-color: #f85149; }
  .event.routing { border-color: #bc8cff; }
  .event.context_inject { border-color: #79c0ff; }
  .event.error { border-color: #f85149; }
  .evt-header { display: flex; gap: 12px; align-items: center; cursor: pointer; padding: 4px 0; }
  .evt-type { font-weight: bold; min-width: 160px; }
  .evt-name { color: #8b949e; }
  .evt-dur { color: #8b949e; font-size: 11px; }
  .evt-body { display: none; padding: 8px 12px; background: #161b22; border-radius: 4px; margin: 4px 0; overflow: auto; max-height: 200px; }
  .evt-body pre { color: #c9d1d9; white-space: pre-wrap; word-break: break-all; }
  .evt-body.open { display: block; }
  .children { padding-left: 20px; }
  .badge { font-size: 10px; padding: 1px 6px; border-radius: 10px; background: #21262d; }
  footer { padding: 12px 24px; color: #8b949e; border-top: 1px solid #30363d; margin-top: 16px; }
  a { color: #58a6ff; }
</style>"""

  private def traceHeader(query: String, sessionId: String, ctx: ExecutionContext): String =
    s"""<header>
  <h1>Loki Engine Trace <span class="badge">${sessionId}…</span></h1>
  <p>Query: "$query" &nbsp;|&nbsp; Hops: ${ctx.hopCount} &nbsp;|&nbsp; Events: ${ctx.trace.length}
  &nbsp;|&nbsp; <a href="/data" target="_blank">Raw JSON</a>
  &nbsp;|&nbsp; <a href="/explain" target="_blank">Explanation</a>
  </p>
</header>"""

  private def traceScript(eventsJson: String): String =
    s"""const DATA = $eventsJson;
const events = DATA.events;
const byId = {};
events.forEach(e => byId[e.event_id] = e);

// Build tree
const rootEvents = events.filter(e => !e.parent_event_id || !byId[e.parent_event_id]);
const childMap = {};
events.forEach(e => {
  if (e.parent_event_id) {
    if (!childMap[e.parent_event_id]) childMap[e.parent_event_id] = [];
    childMap[e.parent_event_id].push(e);
  }
});

function colorForType(t) {
  const m = {hop:'#388bfd',agent_start:'#3fb950',agent_end:'#3fb950',tool_call:'#d29922',tool_result:'#f85149',routing:'#bc8cff',context_inject:'#79c0ff',error:'#f85149'};
  return m[t] || '#8b949e';
}

function renderEvent(e, container) {
  const div = document.createElement('div');
  div.className = 'event ' + e.event_type;
  const header = document.createElement('div');
  header.className = 'evt-header';
  const typeSpan = document.createElement('span');
  typeSpan.className = 'evt-type';
  typeSpan.style.color = colorForType(e.event_type);
  typeSpan.textContent = e.event_type;
  const nameSpan = document.createElement('span');
  nameSpan.className = 'evt-name';
  nameSpan.textContent = e.node_name || e.node_id.substring(0,8);
  const durSpan = document.createElement('span');
  durSpan.className = 'evt-dur';
  durSpan.textContent = e.duration_ms ? e.duration_ms+'ms' : '';
  header.appendChild(typeSpan);
  header.appendChild(nameSpan);
  if (e.duration_ms) header.appendChild(durSpan);
  const body = document.createElement('div');
  body.className = 'evt-body';
  body.innerHTML = '<pre>' + JSON.stringify(e.payload, null, 2) + '</pre>';
  header.addEventListener('click', () => body.classList.toggle('open'));
  div.appendChild(header);
  div.appendChild(body);
  if (childMap[e.event_id] && childMap[e.event_id].length > 0) {
    const childContainer = document.createElement('div');
    childContainer.className = 'children';
    childMap[e.event_id].forEach(c => renderEvent(c, childContainer));
    div.appendChild(childContainer);
  }
  container.appendChild(div);
}

const traceDiv = document.getElementById('trace');
rootEvents.forEach(e => renderEvent(e, traceDiv));"""


  private def openBrowser(url: String): Task[Unit] = ZIO.attemptBlocking {
    val os = sys.props.get("os.name").map(_.toLowerCase).getOrElse("")
    val cmd = if (os.contains("mac")) s"open $url"
              else if (os.contains("win")) s"cmd /c start $url"
              else s"xdg-open $url"
    java.lang.Runtime.getRuntime.exec(cmd)
    ()
  }.ignore

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

  private def escapeHtml(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")
}
