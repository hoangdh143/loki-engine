# Explainable Systems

Loki Engine provides full observability into agent decision-making through structured traces and routing explanations.

## White-Box Routing

Every routing decision is logged with reasoning:

```scala
// All routing events are traced
for (event <- ctx.trace if event.eventType == EventType.Routing) {
  println(s"Intent: ${event.payload("intent")}")
  println(s"Next node: ${event.payload("next_node_id")}")
  println(s"Mode: ${event.payload("mode")}")  // llm | deterministic | decision_table
  println(s"Confidence: ${event.payload.get("confidence")}")
}
```

### Routing Modes

| Mode | Description |
|------|-------------|
| `llm` | LLM-inferred intent from output text |
| `deterministic` | RouteRule condition matched |
| `decision_table` | DecisionTable rule matched |

## Context Selection Explanation

Why was a context node selected?

```scala
for (event <- ctx.trace if event.eventType == EventType.ContextInject) {
  println(s"Selected ${event.payload("count")} context nodes:")
  for (ctx <- event.payload("selected_contexts").asInstanceOf[List[Map[String, Any]]]) {
    println(s"  ${ctx("name")}")
    println(s"    score: ${ctx("score")}")
    println(s"    source: ${ctx("source")}")  // attached | semantic | runtime
    println(s"    hops: ${ctx("hops")}")
    println(s"    reasons: ${ctx("reasons")}")
  }
}
```

### Score Breakdown

Context scores are computed from:

- **semantic**: Cosine similarity between query and context embedding
- **affinity**: Edge weight from `HAS_CONTEXT` 
- **hops**: Path distance from agent (decays by `pathDecay`)
- **priority**: ContextNode priority field
- **recency**: How recently the context was created
- **runtime**: Whether context was materialized during execution

## Tool Call Trace

Full visibility into tool execution:

```scala
for (event <- ctx.trace if event.eventType == EventType.ToolCall) {
  val toolName = event.payload("tool_name")
  val input = event.payload("input")
  println(s"Calling $toolName with $input")
}

for (event <- ctx.trace if event.eventType == EventType.ToolResult) {
  val success = event.payload("success")
  val summary = event.payload("output_summary")
  val duration = event.durationMs
  println(s"$toolName: $success in ${duration}ms")
  println(s"  Output: $summary")
}
```

## Agent Iteration Trace

See exactly what the agent did in each iteration:

```scala
for (event <- ctx.trace if event.eventType == EventType.AgentEnd) {
  val summary = event.payload("text_summary")
  val intent = event.payload("intent")
  val iterations = event.payload("iterations")
  val duration = event.durationMs
  
  println(s"Agent completed in $iterations iteration(s) (${duration}ms)")
  println(s"  Intent: $intent")
  println(s"  Output: $summary")
}
```

## Decision Table Tracing

```scala
for (event <- ctx.trace if event.eventType == EventType.Routing) {
  if (event.payload.get("mode") == Some("decision_table")) {
    val intent = event.payload("intent")
    val matched = intent  // The DecisionRule.name that matched
    println(s"Decision table rule '$matched' triggered")
  }
}
```

## Schema Validation Trace

```scala
for (event <- ctx.trace if event.eventType == EventType.Validation) {
  val success = event.payload("success")
  if (!success) {
    val error = event.payload("error")
    println(s"Validation failed: $error")
  }
}
```

## Explain Run Summary

```scala
import loki.engine.observability.*

val explanation = explainRun(ctx)

println("=== Execution Summary ===")
println(s"Session: ${explanation.sessionId}")
println(s"Hops: ${explanation.hopCount}")
println(s"Paused: ${explanation.paused}")
println()
println("Counts:")
println(s"  Routing decisions: ${explanation.summary.routingDecisionCount}")
println(s"  Tool calls: ${explanation.summary.toolCallCount}")
println(s"  Context injections: ${explanation.summary.contextInjectionCount}")
println(s"  Pause events: ${explanation.summary.pauseEventCount}")

if (explanation.hops.nonEmpty) {
  println()
  println("Hop Details:")
  for (hop <- explanation.hops) {
    println(s"  ${hop.hop}: ${hop.nodeName} - ${hop.summary}")
  }
}

if (explanation.routing.nonEmpty) {
  println()
  println("Routing Decisions:")
  for (r <- explanation.routing) {
    println(s"  ${r.intent} → ${r.nextNodeId} (${r.mode})")
  }
}
```

## Debugging Low Confidence

```scala
// Find low-confidence routing
val lowConf = ctx.trace.collect {
  case e if e.eventType == EventType.Routing =>
    e.payload.get("confidence").collect {
      case c if c.asInstanceOf[Number].doubleValue < 0.7 =>
        (e, c.asInstanceOf[Number].doubleValue)
    }
}.flatten

for ((event, conf) <- lowConf) {
  println(s"Low confidence (${conf*100}%): ${event.nodeName}")
  println(s"  Intent: ${event.payload("intent")}")
  println(s"  Recommendation: Add a route rule for this intent")
}
```

## Runtime Node Provenance

Track where runtime context came from:

```scala
import loki.engine.core.executor.*

val runtimeNodes = runtime.unsafe.run {
  getRuntimeNodes(store, Some(ctx.sessionId))
}.getOrThrow

for (node <- runtimeNodes) {
  val source = node.attributes("origin")  // "runtime"
  val sourceNodeId = node.attributes("source_node_id")
  println(s"${node.name} materialized from $sourceNodeId")
}
```
