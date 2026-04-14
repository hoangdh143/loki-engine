# Core Runtime Maturity

Loki Engine's execution engine provides production-grade reliability.

## Execution Guarantees

### At-Least-Once Execution

Each node executes at least once. With idempotency keys, duplicate executions produce the same result.

```scala
// Idempotent tool - safe to retry
val tool = createTool(
  name = "send_notification",
  callableRef = "mymodule.send",
  executionPolicy = ExecutionPolicy(
    idempotencyKey = "notification_id"
  )
)
```

### Progress Preservation

Persist `ctx.snapshot` externally (database, file) when pausing a workflow, then restore and call `resume()`:

```scala
// After a run that may pause
val ctx = runtime.unsafe.run(app.run(entryNode.nodeId, query)).getOrThrow

if (ctx.isPaused) {
  val snapshot = ctx.snapshot  // Map[String, Any] — serialize with any JSON library
  // ... persist snapshot ...

  // Later: restore and resume
  val restoredCtx = ctx.copy(
    state = ctx.state.copy(status = "running", pendingPause = None)
  )
  val resumed = runtime.unsafe.run {
    executor.resume(entryNodeId, restoredCtx)
  }.getOrThrow
}
```

## Retry Policies

### Per-Tool Retry

```scala
val reliableTool = createTool(
  name = "api_call",
  callableRef = "mymodule.api",
  executionPolicy = ExecutionPolicy(
    retryPolicy = RetryPolicy(
      maxAttempts = 3,
      backoffSeconds = 1.0,
      backoffMultiplier = 2.0
    )
  )
)
```

### Per-Agent Retry

```scala
val agent = createAgent(
  name = "ResilientAgent",
  executionPolicy = ExecutionPolicy(
    retryPolicy = RetryPolicy(
      maxAttempts = 2,
      backoffSeconds = 5.0,
      backoffMultiplier = 1.5
    )
  )
)
```

## Timeout Handling

```scala
val agent = createAgent(
  name = "TimedAgent",
  executionPolicy = ExecutionPolicy(
    timeoutSeconds = Some(60.0)
  )
)
```

## Error Classification

### Transient Errors

Retryable - network issues, rate limits, temporary unavailability:

```scala
// These should trigger retry
case e: java.net.ConnectException => ZIO.retry(...)
case e: RateLimitException => ZIO.retry(...)
```

### Permanent Errors

Non-retryable - validation failures, invalid state:

```scala
// These should fail immediately
case e: ValidationException => ZIO.fail(e)
case e: IllegalArgumentException => ZIO.fail(e)
```

## Graceful Degradation

### Tool Not Found

A call to a tool the agent does not have continues the run with the failure
recorded in the trace, and the model is told what *is* on its belt so it can
self-correct on the next turn instead of retrying the same absent tool:

```scala
val event = ctx.trace.find(_.eventType == EventType.ToolResult).head
// event.payload("success") == false
// event.payload("output_summary") ==
//   "Tool 'read_file' not found. Available tools: read_input, write_file."
```

A node with no tools at all is told to answer directly rather than shown an
empty list. There is deliberately no "did you mean X?" suggestion — see
[Unknown tools name the belt](advanced-configuration.md#tolerating-what-a-local-model-actually-does).

### Routing Failure

```scala
// Falls back to "__END__" if no route matches
// Or uses "default" from routingTable
```

## State Consistency

### Immutable Context

`ExecutionContext` is immutable - safe for concurrent operations:

```scala
val ctx1 = ctx.copy(state = ctx.state.copy(data = Map("x" -> 1)))
val ctx2 = ctx.copy(state = ctx.state.copy(data = Map("x" -> 2)))
// ctx1 and ctx2 are independent
```

### Atomic Updates

All state updates are atomic within a hop:

```scala
// If node execution fails, no state is written
for {
  result <- executeNode(node, ctx)  // Can fail here
  // If above fails, outputs and trace remain unchanged
  newCtx <- ZIO.succeed(ctx.copy(
    outputs = ctx.outputs + (node.nodeId -> result),
    trace = newEvents ::: ctx.trace
  ))
} yield newCtx
```

## Production Checklist

- [ ] Configure retry policies for all external calls
- [ ] Set appropriate timeouts
- [ ] Enable checkpointing for long-running workflows
- [ ] Register error handlers via event hooks
- [ ] Monitor retry events in traces
- [ ] Use idempotency keys for non-idempotent operations
- [ ] Implement circuit breakers for external services

## Event Hooks for Monitoring

```scala
executor.addEventHook((event, ctx) => {
  event.eventType match {
    case EventType.Retry =>
      metrics.increment("retries", Map(
        "node" -> event.nodeName,
        "attempt" -> event.payload("attempt").toString
      ))
    case EventType.Error =>
      alerting.alert(s"Error in ${event.nodeName}: ${event.payload("error")}")
    case _ =>
  }
})
```
