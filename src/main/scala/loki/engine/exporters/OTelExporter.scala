package loki.engine.exporters

import zio.*
import loki.engine.core.executor.*
import loki.engine.core.nodes.EventType
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.time.Instant

/**
 * Exports ExecutionContext traces to any OTLP HTTP/JSON backend
 * (Datadog, Grafana Tempo, Honeycomb, Jaeger, Signoz, etc.)
 *
 * Hierarchy:
 *   Hop event  →  root span
 *     AgentStart/AgentEnd  →  child span
 *       ToolCall/ToolResult  →  grandchild span
 *   All other events (Routing, ContextInject, …) → OTel Events on the nearest parent span
 *
 * OTLP HTTP/JSON endpoint: typically http://<host>:4318/v1/traces
 */
object OTelExporter {

  def exportTrace(
    ctx: ExecutionContext,
    endpoint: String = sys.env.getOrElse("OTEL_ENDPOINT", "http://localhost:4318/v1/traces"),
    serviceName: String = "loki-engine"
  ): Task[Unit] = ZIO.attemptBlocking {
    val payload = buildOtlpPayload(ctx, serviceName)
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create(endpoint))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(payload))
      .build()
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() >= 400) {
      throw new RuntimeException(s"OTEL export failed ${response.statusCode()}: ${response.body().take(200)}")
    }
  }

  private def buildOtlpPayload(ctx: ExecutionContext, serviceName: String): String = {
    val traceId = sessionToTraceId(ctx.sessionId)
    val spans = buildSpans(ctx, traceId)
    val spansJson = spans.mkString(",")
    s"""{
  "resourceSpans":[{
    "resource":{"attributes":[{"key":"service.name","value":{"stringValue":${jStr(serviceName)}}},{"key":"loki.session_id","value":{"stringValue":${jStr(ctx.sessionId)}}}]},
    "scopeSpans":[{
      "scope":{"name":"loki-engine","version":"0.1.0"},
      "spans":[$spansJson]
    }]
  }]
}"""
  }

  private def buildSpans(ctx: ExecutionContext, traceId: String): List[String] = {
    val events = ctx.trace
    val byId = events.map(e => e.eventId -> e).toMap
    val spans = scala.collection.mutable.ListBuffer[String]()

    // Build a parent→children map (eventId → list of child events)
    val children = events.groupBy(_.parentEventId)

    // Identify root events (no parent or parent not in trace)
    val roots = events.filter(e => e.parentEventId.isEmpty || !byId.contains(e.parentEventId.get))

    def toSpanId(id: String): String = {
      val h = id.replace("-", "").take(16).padTo(16, '0')
      h.take(16)
    }

    def toNanos(ts: Instant): Long = ts.getEpochSecond * 1_000_000_000L + ts.getNano

    def makeSpan(e: TraceEvent, parentSpanId: Option[String], depth: Int): Unit = {
      val spanId = toSpanId(e.eventId)
      val startNs = toNanos(e.timestamp)
      val endNs = e.durationMs.map(d => startNs + d * 1_000_000L).getOrElse(startNs + 1_000_000L)
      val name = s"${e.eventType.value}:${e.nodeName}"
      val attrs = attrsFromPayload(e) ++ List(
        s"""{"key":"event.type","value":{"stringValue":${jStr(e.eventType.value)}}}""",
        s"""{"key":"node.id","value":{"stringValue":${jStr(e.nodeId)}}}""",
        s"""{"key":"node.name","value":{"stringValue":${jStr(e.nodeName)}}}"""
      )

      val parentPart = parentSpanId.map(p => s""","parentSpanId":"$p"""").getOrElse("")
      val childEvents = children.getOrElse(Some(e.eventId), Nil)
      val otlpEvents = childEvents.filter(c => !isSpanWorthy(c.eventType)).map { ce =>
        val ceNs = toNanos(ce.timestamp)
        val ceAttrs = attrsFromPayload(ce).mkString(",")
        s"""{"timeUnixNano":"$ceNs","name":${jStr(ce.eventType.value)},"attributes":[$ceAttrs]}"""
      }
      val eventsPart = if (otlpEvents.nonEmpty) s""","events":[${otlpEvents.mkString(",")}]""" else ""

      spans += s"""{
  "traceId":"$traceId",
  "spanId":"$spanId"$parentPart,
  "name":${jStr(name)},
  "kind":3,
  "startTimeUnixNano":"$startNs",
  "endTimeUnixNano":"$endNs",
  "attributes":[${attrs.mkString(",")}]$eventsPart,
  "status":{"code":1}
}"""

      // Recurse into span-worthy children
      childEvents.filter(c => isSpanWorthy(c.eventType)).foreach { c =>
        makeSpan(c, Some(spanId), depth + 1)
      }
    }

    roots.foreach(e => makeSpan(e, None, 0))
    spans.toList
  }

  private def isSpanWorthy(et: EventType): Boolean = et match {
    case EventType.Hop | EventType.AgentStart | EventType.AgentEnd |
         EventType.ToolCall | EventType.ToolResult | EventType.SubgraphEnter |
         EventType.SubgraphExit | EventType.ApprovalTask => true
    case _ => false
  }

  private def attrsFromPayload(e: TraceEvent): List[String] = {
    e.payload.flatMap {
      case (k, v: String) => List(s"""{"key":${jStr(k)},"value":{"stringValue":${jStr(v)}}}""")
      case (k, v: Boolean) => List(s"""{"key":${jStr(k)},"value":{"boolValue":$v}}""")
      case (k, v: Number) => List(s"""{"key":${jStr(k)},"value":{"doubleValue":${v.doubleValue()}}}""")
      case (k, v: Int) => List(s"""{"key":${jStr(k)},"value":{"intValue":$v}}""")
      case (k, v) if v != null => List(s"""{"key":${jStr(k)},"value":{"stringValue":${jStr(v.toString)}}}""")
      case _ => Nil
    }.toList
  }

  private def sessionToTraceId(sessionId: String): String =
    sessionId.replace("-", "").padTo(32, '0').take(32)

  private def jStr(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t") + "\""
}
