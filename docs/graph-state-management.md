# Graph State Management

How Loki Engine manages state across workflow execution.

## ExecutionContext State

The `ExecutionContext` carries all state during execution:

```scala
case class ExecutionContext(
  query: Any,                              // Input query
  sessionId: String,                       // Unique session identifier
  outputs: Map[String, Any],               // nodeId → output
  trace: List[TraceEvent],                 // Execution history
  state: WorkflowState,                    // Mutable workflow state
  maxHops: Int = 20,                       // Execution depth limit
  hopCount: Int = 0,                       // Current depth
  extraMessages: List[Map[String, Any]] = Nil,
  allowedTools: Option[Set[String]] = None
)
```

## WorkflowState

The mutable state container:

```scala
case class WorkflowState(
  data: Map[String, Any] = Map.empty,      // Arbitrary key-value data
  schema: Map[String, Any] = Map.empty,     // State validation schema
  status: String = "running",              // running | paused | completed
  pendingPause: Option[WorkflowPause] = None,
  inbox: Map[String, ApprovalTask] = Map.empty,
  idempotencyCache: Map[String, Any] = Map.empty,
  metadata: Map[String, Any] = Map.empty
)
```

## State Operations

### Reading State

```scala
// From node execution
val result = for {
  userName <- ZIO.succeed(ctx.state.data.get("user_name"))
  tier <- ZIO.succeed(ctx.state.data.getOrElse("tier", "free"))
} yield s"Hello $userName ($tier)"
```

### Writing State

```scala
// Agents write to state
ctx.state.data.get("last_output")  // Written by executor after each node

// Via route rules
RouteRule(
  source = "result",
  path = "classification",
  operator = "equals",
  value = "urgent",
  targetNodeId = urgentHandler.nodeId
)

// Via workflow data
val updatedCtx = ctx.copy(
  state = ctx.state.copy(
    data = ctx.state.data + ("user_id" -> userId, "session_active" -> true)
  )
)
```

### State Schema Validation

```scala
val agent = createAgent(
  name = "ValidatedAgent",
  stateSchema = Map(
    "type" -> "object",
    "properties" -> Map(
      "user_id" -> Map("type" -> "string"),
      "action" -> Map("type" -> "string", "enum" -> List("read", "write", "delete"))
    ),
    "required" -> List("user_id")
  )
)
```

## Routing Based on State

### Route Rules

```scala
val agent = createAgent(
  name = "OrderProcessor",
  routeRules = List(
    RouteRule(
      name = "high_value",
      source = "state",
      path = "order.value",
      operator = "gt",
      value = 1000,
      targetNodeId = manager.nodeId,
      priority = 10
    ),
    RouteRule(
      name = "international",
      source = "state",
      path = "order.country",
      operator = "in",
      value = List("CA", "US", "MX"),
      targetNodeId = naTeam.nodeId,
      priority = 5
    )
  )
)
```

### Decision Tables

```scala
val agent = createAgent(
  name = "TicketRouter",
  decisionTable = Some(DecisionTable(
    name = "Support Routing",
    rules = List(
      DecisionRule(
        name = "billing_urgent",
        conditions = List(
          ConstraintRule(source = "state", path = "department", operator = "equals", value = "billing"),
          ConstraintRule(source = "state", path = "priority", operator = "equals", value = "high")
        ),
        targetNodeId = billingUrgent.nodeId,
        priority = 100
      ),
      DecisionRule(
        name = "billing_normal",
        conditions = List(
          ConstraintRule(source = "state", path = "department", operator = "equals", value = "billing")
        ),
        targetNodeId = billingNormal.nodeId,
        priority = 50
      )
    ),
    defaultTargetId = general.nodeId,
    strict = false
  ))
)
```

## Last Output Tracking

The executor automatically tracks outputs:

```scala
// After each node execution
ctx.outputs(node.nodeId) = result

// Also updates
ctx.state.data("_last_node_id") = node.nodeId
ctx.state.data("_last_output") = result
```

## State Snapshots

```scala
// Snapshot current state — returns Map[String, Any]
val snapshot: Map[String, Any] = ctx.snapshot

// snapshot contains:
// - "query"       → the original query
// - "session_id"  → unique session identifier
// - "outputs"     → Map of nodeId → output
// - "trace"       → List of trace events as Maps
// - "state"       → Map with data, inbox, status, etc.

// Serialize with any JSON library (uPickle, play-json, jackson, etc.)
// and persist to your datastore
```

## Materialized Context

Output can be automatically written back to the graph:

```scala
// After tool execution, result can be materialized
// This creates a ContextNode in the graph with:
// - group_id = session_id
// - attributes.origin = "runtime"

// Later retrieval
val runtimeNodes = runtime.unsafe.run {
  store.listNodes(nodeType = Some(NodeType.Context), groupId = Some(sessionId))
}.getOrThrow.filter(_.attributes.get("origin") == Some("runtime"))
```

## Session Isolation

Each execution gets a unique session:

```scala
val ctx1 = runtime.unsafe.run(app.run(agent.nodeId, query1)).getOrThrow
val ctx2 = runtime.unsafe.run(app.run(agent.nodeId, query2)).getOrThrow

// Different sessions = isolated state
assert(ctx1.sessionId != ctx2.sessionId)
```

## Multi-Tenancy via GroupId

```scala
// Nodes can be partitioned by group
val agent = createAgent(name = "Agent", groupId = "tenant-123")

// Query within group
val tenantNodes = runtime.unsafe.run {
  store.listNodes(groupId = Some("tenant-123"))
}

// Runtime context nodes inherit session as group
// (materialized outputs have groupId = sessionId)
```
