# First Graph

A complete walkthrough of building a multi-step agent workflow.

## Overview

We'll build a research pipeline with:
1. A triage agent that routes requests
2. A web search tool
3. A summarizer agent
4. An approval step for sensitive queries

## Setup

```scala
import zio.*
import loki.engine.app.*
import loki.engine.core.store.*
import loki.engine.core.nodes.*
import loki.engine.core.executor.*
import loki.engine.backend.*
```

## Create the Graph

```scala
val runtime = Runtime.default

Unsafe.unsafe { implicit u =>
  val store = runtime.unsafe.run(GraphStore.make).getOrThrow
  
  // Create agents
  val triage = createAgent(
    name = "TriageAgent",
    model = "claude-sonnet-4-6",
    systemPrompt = "Classify the user's request as: research, coding, or general",
    routingTable = Map(
      "research" -> "researcher.nodeId",
      "coding" -> "coder.nodeId", 
      "default" -> "__END__"
    )
  )
  
  val researcher = createAgent(
    name = "Researcher",
    model = "claude-sonnet-4-6",
    systemPrompt = "You are a research assistant. Search for information and summarize findings."
  )
  
  val coder = createAgent(
    name = "Coder",
    model = "claude-sonnet-4-6", 
    systemPrompt = "You are a coding assistant. Write clean, well-documented code."
  )
  
  // Create approval node
  val approval = ApprovalNode(
    name = "ManagerApproval",
    instructions = "A manager must approve sensitive requests.",
    assignees = List("mgr-1"),
    approvedTargetId = "researcher.nodeId",
    rejectedTargetId = "__END__"
  )
  
  // Create tools
  val searchTool = createTool(
    name = "web_search",
    callableRef = "loki.engine.tools.web_search.search",
    description = "Search the web",
    inputSchema = Map(
      "type" -> "object",
      "properties" -> Map(
        "query" -> Map("type" -> "string")
      ),
      "required" -> List("query")
    )
  )
  
  // Create context
  val researchGuidelines = createContext(
    name = "Research Guidelines",
    content = "Always cite sources. Verify facts before presenting."
  )
  
  // Build the app
  val app = new GraphApp(store)
  
  // Add all nodes
  runtime.unsafe.run(app.addAgent("TriageAgent", triage))
  runtime.unsafe.run(app.addAgent("Researcher", researcher))
  runtime.unsafe.run(app.addAgent("Coder", coder))
  runtime.unsafe.run(app.addTool("web_search", searchTool))
  runtime.unsafe.run(app.addContext("Guidelines", researchGuidelines))
  
  // Connect nodes
  runtime.unsafe.run(app.connectTool(researcher.nodeId, searchTool.nodeId))
  runtime.unsafe.run(app.connectContext(researcher.nodeId, researchGuidelines.nodeId))
  runtime.unsafe.run(app.delegate(triage.nodeId, researcher.nodeId))
  runtime.unsafe.run(app.delegate(triage.nodeId, coder.nodeId))
  
  // Run the pipeline
  val result = runtime.unsafe.run {
    app.run(triage.nodeId, "Research the latest AI trends in 2024")
  }.getOrThrow
  
  println(s"Completed in ${result.hopCount} hops")
  println(s"Trace events: ${result.trace.length}")
}
```

## Understanding the Trace

Print the execution trace:

```scala
import loki.engine.core.executor.*
printTrace(result)
```

Output:
```
Session a1b2c3d4  'Research the latest AI trends'
═══════════════════════════════════════════════════════════════
 HOP 1  AGENT  TriageAgent
   routing  research → researcher.nodeId
 HOP 2  AGENT  Researcher
   tools: web_search
   context_inject  Research Guidelines  (1 nodes)
   tool_call  web_search  {"query": "AI trends 2024"}
     tool_result  web_search  ok  "Results: ..."  [45ms]
   agent_end  "Based on the research..."  intent=default  [120ms]
═══════════════════════════════════════════════════════════════
Total: 2 hops · 1 agent_end events · 120ms
```

## Adding Approval Flow

For sensitive operations:

```scala
// Create a sensitive query that needs approval
val sensitiveResult = runtime.unsafe.run {
  app.run(approval.nodeId, "Delete all user data")
}.getOrThrow

// Check the inbox
val pendingTasks = sensitiveResult.state.inbox.values
  .filter(_.status == "pending")
  .toList

// Approve or reject
val approvedCtx = sensitiveResult.copy(
  state = sensitiveResult.state.copy(
    inbox = sensitiveResult.state.inbox.map { case (id, task) =>
      id -> task.copy(status = "approved", resolvedAt = Some(java.time.Instant.now()))
    },
    data = sensitiveResult.state.data + ("approval" -> Map(
      "approved" -> true,
      "assigned_to" -> "mgr-1"
    ))
  )
)

// Resume execution
val resumed = runtime.unsafe.run {
  app.getExecutor.resume(approval.nodeId, approvedCtx)
}.getOrThrow
```

## Batch Processing

Process multiple items concurrently:

```scala
import loki.engine.core.executor.BatchExecutor

val documents = List(
  "Document 1 about AI...",
  "Document 2 about ML...",
  "Document 3 about DL..."
)

val batchExec = new BatchExecutor(store, app.getExecutor, concurrency = 5)

val batchResult = runtime.unsafe.run {
  batchExec.run(
    agentNodeId = researcher.nodeId,
    items = documents,
    queryFn = (doc: Any) => s"Summarize: $doc",
    contextFn = Some((doc: Any) => doc.toString),
    reduceFn = Some((outputs: List[Any]) => outputs.mkString("\n---\n"))
  )
}.getOrThrow

println(s"Status: ${batchResult.status}")
println(f"Progress: ${batchResult.progress * 100}%.1f%%")
```

## Next Steps

- [Workflow Patterns](./workflow-patterns.md) for common patterns
- [Observability](./observability.md) for trace analysis
- [Advanced Configuration](./advanced-configuration.md) for customization
