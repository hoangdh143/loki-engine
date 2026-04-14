# Workflow Patterns

Common workflow patterns using Loki Engine.

## Sequential Pipeline

The most common pattern: a chain of agents where each routes output to the next via `routingTable`.

```scala
// Create pipeline agents — each routes to the next by default
val formatter = createAgent(name = "Formatter", ...)

val processor = createAgent(
  name = "Processor",
  routingTable = Some(Map("default" -> formatter.nodeId))
)

val collector = createAgent(
  name = "Collector",
  routingTable = Some(Map("default" -> processor.nodeId))
)

// Add all nodes to the store
runtime.unsafe.run(app.addAgent("Collector", collector)).getOrThrow
runtime.unsafe.run(app.addAgent("Processor", processor)).getOrThrow
runtime.unsafe.run(app.addAgent("Formatter", formatter)).getOrThrow

// Run — will traverse Collector → Processor → Formatter
val result = runtime.unsafe.run(app.run(collector.nodeId, userQuery)).getOrThrow
```

## Parallel Fan-out

Distribute work to multiple agents concurrently, then merge results.

```scala
// Create parallel workers
val searcher = createAgent(name = "Searcher", ...)
val analyzer = createAgent(name = "Analyzer", ...)
val reporter = createAgent(name = "Reporter", ...)

// Connect all to a coordinator
runtime.unsafe.run(app.delegate(coordinator.nodeId, searcher.nodeId))
runtime.unsafe.run(app.delegate(coordinator.nodeId, analyzer.nodeId))
runtime.unsafe.run(app.delegate(coordinator.nodeId, reporter.nodeId))

// Run with parallel strategy
val result = runtime.unsafe.run {
  app.run(coordinator.nodeId, query, strategy = "parallel")
}
```

## Conditional Routing

Route based on workflow state or output content.

```scala
val agent = createAgent(
  name = "Router",
  routeRules = List(
    RouteRule(
      name = "escalate_high_value",
      source = "state",
      path = "order.value",
      operator = "gt",
      value = 10000,
      targetNodeId = manager.nodeId,
      priority = 10
    ),
    RouteRule(
      name = "escalate_fraud",
      source = "result",
      path = "fraud_score",
      operator = "gte",
      value = 0.8,
      targetNodeId = fraudTeam.nodeId,
      priority = 9
    )
  )
)
```

## Decision Table

For complex multi-condition routing:

```scala
val agent = createAgent(
  name = "Classifier",
  decisionTable = Some(DecisionTable(
    name = "Request Classifier",
    rules = List(
      DecisionRule(
        name = "urgent_billing",
        conditions = List(
          ConstraintRule(source = "state", path = "type", operator = "equals", value = "billing"),
          ConstraintRule(source = "state", path = "urgent", operator = "equals", value = true)
        ),
        targetNodeId = urgentBilling.nodeId,
        priority = 100
      ),
      DecisionRule(
        name = "standard_billing", 
        conditions = List(
          ConstraintRule(source = "state", path = "type", operator = "equals", value = "billing")
        ),
        targetNodeId = normalBilling.nodeId,
        priority = 50
      )
    ),
    defaultTargetId = general.nodeId,
    strict = false
  ))
)
```

## Approval Gate

Pause workflow for human approval:

```scala
val approval = ApprovalNode(
  name = "ManagerApproval",
  instructions = "Manager must approve budget changes over $10,000",
  assignees = List("mgr-1", "mgr-2"),
  assignmentMode = "any",
  slaSeconds = Some(86400),  // 24 hours
  escalationTarget = "escalationMgr.nodeId",
  approvedTargetId = approvedPath.nodeId,
  rejectedTargetId = rejectedPath.nodeId
)

// Run until approval
val paused = runtime.unsafe.run(app.run(approval.nodeId, request))

// Check pending approvals
val pending = paused.state.inbox.values.filter(_.status == "pending")

// User approves via state
val approved = paused.copy(
  state = paused.state.copy(
    inbox = paused.state.inbox.map { case (id, task) =>
      if (id == pending.head.taskId) 
        task.copy(status = "approved", resolvedAt = Some(Instant.now()))
      else task
    },
    data = paused.state.data + ("approval" -> Map("approved" -> true))
  )
)

// Resume
val resumed = runtime.unsafe.run(app.getExecutor.resume(approval.nodeId, approved)).getOrThrow
```

## Subgraph Composition

Reuse common workflows as subgraphs:

```scala
// Create a sub-graph
val subSearch = createAgent(name = "SubSearch", ...)
val subRank = createAgent(name = "SubRank", ...)

runtime.unsafe.run(app.connectTool(subSearch.nodeId, subRank.nodeId))

// Create a graph node pointing to the sub
val graphNode = GraphNode(
  name = "SearchPipeline",
  entryNodeId = subSearch.nodeId,
  exitNodeId = subRank.nodeId
)

// Use in main graph
runtime.unsafe.run(app.run(graphNode.nodeId, query))
```

## Context-Aware Agent

Attach relevant knowledge to agents:

```scala
// Create contextual knowledge
val policy = createContext(
  name = "Company Policy",
  content = "All refunds require manager approval for orders over $500."
)

val faq = createContext(
  name = "FAQ",
  content = "We offer 30-day returns on all products."
)

// Connect with weights (higher = more relevant)
runtime.unsafe.run(app.connectContext(agent.nodeId, policy.nodeId, weight = Some(0.9)))
runtime.unsafe.run(app.connectContext(agent.nodeId, faq.nodeId, weight = Some(0.7)))
```

## Long-Running Workflows

Pause and resume workflows by persisting the `ExecutionContext.snapshot`:

```scala
// Run until paused (e.g., at an ApprovalNode)
val ctx = runtime.unsafe.run(app.run(entryNode.nodeId, query)).getOrThrow

if (ctx.isPaused) {
  // Persist ctx.snapshot externally (database, file, etc.)
  val snapshot = ctx.snapshot
  // ... save snapshot to your datastore ...

  // Later: restore and resume
  val restoredCtx = ctx.copy(
    state = ctx.state.copy(
      status = "running",
      pendingPause = None
    )
  )
  val resumed = runtime.unsafe.run {
    app.getExecutor.resume(entryNode.nodeId, restoredCtx)
  }.getOrThrow
}
```

## Batch Processing

Process large datasets efficiently:

```scala
val batchExec = new BatchExecutor(store, executor, concurrency = 10)

val results = runtime.unsafe.run {
  batchExec.run(
    agentNodeId = processor.nodeId,
    items = documents,
    queryFn = (doc: Any) => s"Process: $doc",
    contextFn = Some((doc: Any) => doc.toString),
    reduceFn = Some((outputs: List[Any]) => outputs.mkString("\n")),
    checkpoint = true
  )
}.getOrThrow

// Resume if interrupted — pass the BatchRun, not just the runId
if (results.status != BatchStatus.Completed) {
  val resumed = runtime.unsafe.run {
    batchExec.resume(
      existingRun = results,
      items = documents,
      queryFn = (doc: Any) => s"Process: $doc"
    )
  }.getOrThrow
}
```

## Error Handling with Retry

Configure retry policies:

```scala
val tool = createTool(
  name = "unreliable_api",
  callableRef = "mymodule.api_call",
  executionPolicy = ExecutionPolicy(
    retryPolicy = RetryPolicy(
      maxAttempts = 3,
      backoffSeconds = 1.0,
      backoffMultiplier = 2.0
    )
  )
)
```

## Idempotent Operations

Prevent duplicate work:

```scala
val tool = createTool(
  name = "payment",
  callableRef = "mymodule.process_payment",
  executionPolicy = ExecutionPolicy(
    idempotencyKey = "transaction_id"
  )
)
```
