# Long-Running Workflows

Loki Engine supports workflows that span hours or days with pause/resume semantics, human approval gates, and resumable batch jobs.

## Pause and Resume

### Automatic Pause via ApprovalNode

```scala
import loki.engine.core.nodes.*
import loki.engine.core.executor.*

val approval = ApprovalNode(
  name = "ManagerApproval",
  instructions = "Please review and approve or reject this request",
  assignees = List("manager@company.com"),
  approvedTargetId = "processingNodeId",
  rejectedTargetId = "rejectionNodeId"
)

// Run will pause at the approval node
val paused = runtime.unsafe.run(app.run(approval.nodeId, request)).getOrThrow

println(s"Paused: ${paused.isPaused}")
println(s"Reason: ${paused.state.pendingPause.map(_.reason)}")
println(s"Approval tasks: ${paused.state.inbox.size}")
```

### Manual Resume

```scala
// Inject the approval decision into state
val approved = paused.copy(
  state = paused.state.copy(
    data = paused.state.data + ("approval" -> Map(
      "approved" -> true,
      "assigned_to" -> "manager@company.com"
    )),
    status = "running",
    pendingPause = None
  )
)

// Resume from the approval node
val resumed = runtime.unsafe.run {
  executor.resume(approval.nodeId, approved)
}.getOrThrow
```

## SLA and Escalation

```scala
val approval = ApprovalNode(
  name = "UrgentApproval",
  instructions = "URGENT: Approve within 1 hour",
  assignees = List("on-call-manager"),
  slaSeconds = Some(3600),              // 1 hour SLA
  escalationTarget = "escalationMgr",  // Escalate if SLA is missed
  approvedTargetId = "approvedPath",
  rejectedTargetId = "rejectedPath"
)
```

When the SLA deadline passes, an `approval_task` trace event with `due_at` is emitted and can be used to trigger external escalation logic.

## Manual Workflow Pause

Agents can also pause themselves via `pauseBefore` / `pauseAfter`:

```scala
val agent = AgentNode(
  name = "ReviewAgent",
  pauseAfter = true  // Pause after executing, before routing
)
```

Or conditionally via a route rule:

```scala
RouteRule(
  name = "pause_on_risk",
  source = "result",
  path = "risk_level",
  operator = "equals",
  value = "high",
  targetNodeId = "reviewNodeId",
  pauseOnMatch = true,
  priority = 100
)
```

## State Preservation

All workflow state is carried in `ExecutionContext`:

```scala
case class ExecutionContext(
  query: Any,
  sessionId: String,
  outputs: Map[String, Any],        // All node outputs
  trace: List[TraceEvent],          // Full execution history
  state: WorkflowState,             // Mutable workflow state
  hopCount: Int,
  maxHops: Int
)

case class WorkflowState(
  data: Map[String, Any],           // Arbitrary workflow data
  schema: Map[String, Any],         // Optional state validation schema
  status: String,                   // "running" | "paused" | "completed"
  pendingPause: Option[WorkflowPause],
  inbox: Map[String, ApprovalTask], // Pending approval tasks
  idempotencyCache: Map[String, Any]
)
```

Serialize the context snapshot for external storage:

```scala
val snapshot = ctx.snapshot  // Map[String, Any]

// Persist to your datastore (as JSON, a database row, etc.)
```

## Timer-Based Workflows

Combine with external schedulers:

```scala
// 1. Start workflow
val ctx = runtime.unsafe.run(app.run(entry.nodeId, input)).getOrThrow
val sessionId = ctx.sessionId

// 2. Persist ctx.snapshot externally (database, file, etc.)
// ...

// 3. External scheduler wakes the workflow later
// Restore the context from your datastore and resume

val resumed = runtime.unsafe.run {
  executor.resume(entry.nodeId, restoredCtx)
}.getOrThrow
```

## Resumable Batch Jobs

`BatchExecutor.resume()` takes the original `BatchRun` and skips already-completed items:

```scala
val batchExec = new BatchExecutor(store, executor, concurrency = 10)

// Start batch
val run = runtime.unsafe.run {
  batchExec.run(
    agentNodeId = agent.nodeId,
    items = largeDataset,
    queryFn = itemToQuery,
    checkpoint = true
  )
}.getOrThrow

// If interrupted or partially failed, resume
// Items with status == Completed are skipped automatically
if (run.status != BatchStatus.Completed) {
  val completed = runtime.unsafe.run {
    batchExec.resume(
      existingRun = run,        // Pass the BatchRun, not just runId
      items = largeDataset,
      queryFn = itemToQuery
    )
  }.getOrThrow

  println(s"Final status: ${completed.status}")
}
```

## Idempotency

Use idempotency keys on tools to safely retry without duplicate effects:

```scala
val tool = createTool(
  name = "send_email",
  callableRef = "mymodule.send_email",
  executionPolicy = ExecutionPolicy(
    idempotencyKey = "email_message_id"
  )
)
```

## Best Practices

1. **Persist `ctx.snapshot`** externally when pausing for human review
2. **Use `ApprovalNode`** for structured human-in-the-loop flows with SLA tracking
3. **Use `BatchExecutor.resume(run, ...)`** for resuming interrupted batch jobs — pass the `BatchRun`, not just the run ID
4. **Set `idempotencyKey`** on tools with external side effects to prevent duplicates on retry
5. **Inspect `paused.state.inbox`** to surface approval tasks to users via your own UI
