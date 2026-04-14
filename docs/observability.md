# Observability

Loki Engine provides comprehensive observability through structured trace events, analysis utilities, a terminal trace printer, a browser visualizer, and OpenTelemetry export.

## Trace Events

Every execution produces typed trace events stored in `ExecutionContext.trace`:

```scala
case class TraceEvent(
  eventType: EventType,           // Typed event identifier
  sessionId: String,              // Execution session
  nodeId: String,                 // Node that produced event
  nodeName: String,               // Human-readable name
  timestamp: Instant,             // When event occurred
  payload: Map[String, Any],      // Event-specific data
  eventId: String,                // Unique event ID
  parentEventId: Option[String],  // Parent event for hierarchy
  durationMs: Option[Long]        // Wall-clock duration
)
```

### Event Types

| Event | Description | Key Payload Fields |
|-------|-------------|-------------------|
| `hop` | Graph traversal step | `hop`, `node_type`, `summary` |
| `agent_start` | Agent execution begins | `model`, `tools`, `context`, `query` |
| `agent_end` | Agent execution completes | `text_summary`, `intent`, `iterations` |
| `tool_call` | Tool invocation | `tool_name`, `callable_ref`, `input` |
| `tool_result` | Tool execution result | `tool_name`, `success`, `output_summary` |
| `routing` | Routing decision | `intent`, `next_node_id`, `confidence`, `mode` |
| `context_inject` | Context assembled | `context_names`, `count`, `selected_contexts` |
| `pause` | Workflow paused | `reason`, `token`, `waiting_for` |
| `resume` | Workflow resumed | `token` |
| `checkpoint` | State persisted | `checkpoint_node_id` |
| `validation` | Schema validation | `success`, `error` |
| `approval_task` | Approval task created | `task_id`, `assignees`, `due_at` |
| `retry` | Operation retry | `attempt`, `max_attempts`, `error` |
| `subgraph_enter/exit` | Subgraph boundaries | `entry_node_id`, `summary` |
| `error` | Execution error | `error`, `error_type` |

---

## Terminal Trace

```scala
import loki.engine.core.executor.*
printTrace(ctx)
```

Output example:
```
Session a1b2c3d4  'Research AI trends'
════════════════════════════════════════════════════════════════════════
 HOP 1  AGENT  Researcher
   tools: web_search, document_search
   context_inject  Guidelines, Policy  (2 nodes)
   tool_call  web_search  {"query": "AI trends 2024"}
     tool_result  web_search  ok  'Results: ...'  [45ms]
   routing  synthesis → synthesizer-node-id
 HOP 2  AGENT  Synthesizer
   agent_end  'Based on the research...'  intent=default  [120ms]
════════════════════════════════════════════════════════════════════════
Total: 2 hops · 2 agent_end events · 165ms
```

---

## Browser Trace Visualizer

Launch an interactive HTML trace viewer in the browser:

```scala
import loki.engine.viz.TraceServer
import zio.*

// Blocking — serves until process is killed; auto-opens the browser
runtime.unsafe.run(TraceServer.serve(ctx, port = 8765)).getOrThrow

// Non-blocking — returns the server fiber for manual control
val fiber = runtime.unsafe.run(TraceServer.serveBackground(ctx, port = 8765)).getOrThrow
```

Routes:

| Path | Content |
|------|---------|
| `GET /` | Interactive color-coded event tree (click to expand payloads) |
| `GET /data` | Full trace as JSON |
| `GET /explain` | `RunExplanation` as JSON |

---

## OpenTelemetry Export

Export traces to any OTLP HTTP/JSON backend (Grafana Tempo, Honeycomb, Jaeger, Datadog, Signoz, …):

```scala
import loki.engine.exporters.OTelExporter

runtime.unsafe.run {
  OTelExporter.exportTrace(
    ctx,
    endpoint = "http://localhost:4318/v1/traces",  // or OTEL_ENDPOINT env var
    serviceName = "my-agent"
  )
}.getOrThrow
```

Span hierarchy in the exported trace:
```
hop (root span)
  └── agent_start / agent_end (child span)
        └── tool_call / tool_result (grandchild span)
              routing, context_inject → OTel Events on the nearest parent span
```

No OTEL Java SDK dependency is required — the exporter sends a plain OTLP HTTP/JSON payload using `java.net.http.HttpClient`.

---

## Analyzing Traces

### Extract Trace View

```scala
import loki.engine.observability.*
val view = extractTraceView(ctx.trace)

println(s"Hops: ${view.hops.length}")
println(s"Agent calls: ${view.agentEnds.length}")
println(s"Tool calls: ${view.toolCalls.length}")
val toolErrors = view.toolResults.count(!_.payload.get("success").contains(true))
println(s"Tool errors: $toolErrors")
```

### Explain Run

```scala
val explanation = explainRun(ctx)

println(s"Session: ${explanation.sessionId}")
println(s"Hops: ${explanation.hopCount}")
println(s"Paused: ${explanation.paused}")
println(s"Tool calls: ${explanation.summary.toolCallCount}")
println(s"Routing decisions: ${explanation.summary.routingDecisionCount}")

for (tc <- explanation.toolCalls) {
  println(s"  ${tc.toolName}: ${tc.input}")
}

for (r <- explanation.routing) {
  println(s"  ${r.intent.getOrElse("?")} → ${r.nextNodeId.getOrElse("__END__")} (${r.mode})")
}
```

### Custom Analysis

```scala
// Find all low-confidence routing decisions
val lowConfRoutes = ctx.trace.filter { e =>
  e.eventType == EventType.Routing &&
  e.payload.get("confidence").exists { c =>
    c.asInstanceOf[Number].doubleValue < 0.7
  }
}

// Find all tool errors
val toolErrors = ctx.trace.filter { e =>
  e.eventType == EventType.ToolResult &&
  e.payload.get("success").contains(false)
}

// Total agent execution time
val totalAgentMs = ctx.trace.collect {
  case e if e.eventType == EventType.AgentEnd => e.durationMs.getOrElse(0L)
}.sum
```

---

## Context Scoring

Inspect why specific context was selected:

```scala
for (ctxExplain <- explanation.context) {
  println(s"Context for ${ctxExplain.nodeId}:")
  for (selected <- ctxExplain.selectedContexts) {
    println(s"  ${selected("name")}: score=${selected("score")}")
    println(s"    source=${selected("source")}, hops=${selected("hops")}")
    println(s"    reasons: ${selected("reasons").asInstanceOf[List[String]].mkString(", ")}")
  }
}
```

---

## Session Utilities

### Get Runtime Nodes

After execution, runtime-materialized context nodes are tagged with `origin: "runtime"`:

```scala
import loki.engine.core.executor.*
val runtimeNodes = runtime.unsafe.run {
  getRuntimeNodes(store, Some(ctx.sessionId))
}.getOrThrow

for (node <- runtimeNodes) {
  println(s"${node.name}: ${node.content.take(100)}")
}
```

### Cleanup Session

Remove all runtime nodes from a session:

```scala
// Soft expire (preserves history, nodes become invalid)
val count = runtime.unsafe.run {
  cleanupSession(store, ctx.sessionId)
}.getOrThrow

// Hard delete (frees memory completely)
val count = runtime.unsafe.run {
  cleanupSession(store, ctx.sessionId, hard = true)
}.getOrThrow

println(s"Cleaned $count nodes")
```

---

## Custom Event Hooks

```scala
class MyObserver {
  def onEvent(event: TraceEvent, ctx: ExecutionContext): Unit = {
    event.eventType match {
      case EventType.ToolResult if !event.payload.get("success").contains(true) =>
        alertSlack(s"Tool ${event.nodeName} failed: ${event.payload.get("output_summary")}")
      case EventType.AgentEnd =>
        recordMetric("agent_duration_ms",
          event.durationMs.getOrElse(0L),
          Map("agent" -> event.nodeName))
      case EventType.Error =>
        logger.error(s"Error in ${event.nodeName}: ${event.payload.get("error")}")
      case _ =>
    }
  }
}

executor.addEventHook((event, ctx) => myObserver.onEvent(event, ctx))
```
