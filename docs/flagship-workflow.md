# Flagship Workflow

A complete production-ready workflow demonstrating all Loki Engine features.

## Overview

This workflow implements a document processing pipeline with:
- Intelligent routing based on document type
- Parallel processing for multiple sections
- Approval for sensitive documents with SLA
- Batch processing for multiple documents
- Full observability

## Implementation

```scala
import zio.*
import loki.engine.app.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.core.executor.*
import loki.engine.backend.*
import java.time.Instant

object FlagshipWorkflow {
  
  def buildGraph(store: GraphStore, backend: LLMBackend): (GraphApp, String) = {
    val app = new GraphApp(store, backend = Some(backend))
    
    // === Nodes — define processors first so their nodeIds are available for routing ===
    
    val contractProcessor = createAgent(
      name = "ContractProcessor",
      model = "claude-sonnet-4-6",
      systemPrompt = "Extract key terms and flag issues in contracts."
    )
    
    val invoiceProcessor = createAgent(
      name = "InvoiceProcessor",
      model = "claude-sonnet-4-6",
      systemPrompt = "Extract line items and total from invoices."
    )
    
    val reportGenerator = createAgent(
      name = "ReportGenerator",
      model = "claude-sonnet-4-6",
      systemPrompt = "Summarize and format reports."
    )
    
    // Approval node for high-value contracts
    val approval = ApprovalNode(
      name = "ManagerApproval",
      instructions = "Please review and approve high-value contracts",
      assignees = List("legal-team"),
      slaSeconds = Some(86400),
      approvedTargetId = contractProcessor.nodeId,
      rejectedTargetId = "__END__"
    )
    
    // Router agent — classifies incoming documents and routes them
    val router = createAgent(
      name = "DocumentRouter",
      model = "claude-sonnet-4-6",
      systemPrompt = "Classify the document type and route appropriately.",
      routingTable = Some(Map(
        "contract" -> approval.nodeId,
        "invoice" -> invoiceProcessor.nodeId,
        "report" -> reportGenerator.nodeId,
        "default" -> "__END__"
      ))
    ).copy(
      routeRules = List(
        // High-value documents always go to approval regardless of type
        RouteRule(
          name = "high_value",
          source = "state",
          path = "document.value",
          operator = "gt",
          value = 100000,
          targetNodeId = approval.nodeId,
          priority = 20
        )
      )
    )
    
    // Tools
    val ocrTool = createTool(
      name = "ocr",
      callableRef = "loki.engine.tools.ocr.extract",
      description = "Extract text from documents",
      inputSchema = Map(
        "type" -> "object",
        "properties" -> Map("document_url" -> Map("type" -> "string")),
        "required" -> List("document_url")
      )
    )
    
    val saveTool = createTool(
      name = "save_result",
      callableRef = "loki.engine.tools.storage.save",
      description = "Save processing result",
      inputSchema = Map(
        "type" -> "object",
        "properties" -> Map("data" -> Map("type" -> "string")),
        "required" -> List("data")
      )
    )
    
    // Context for company policy
    val policy = createContext(
      name = "Processing Policy",
      content = "Always verify amounts over $10,000. Flag contracts over $100,000 for review."
    )
    
    // === Register ===
    
    runtime.unsafe.run(app.addAgent("Router", router))
    runtime.unsafe.run(app.addAgent("ContractProcessor", contractProcessor))
    runtime.unsafe.run(app.addAgent("InvoiceProcessor", invoiceProcessor))
    runtime.unsafe.run(app.addAgent("ReportGenerator", reportGenerator))
    runtime.unsafe.run(app.addTool("ocr", ocrTool))
    runtime.unsafe.run(app.addTool("save", saveTool))
    runtime.unsafe.run(app.addContext("Policy", policy))
    
    // === Connect ===
    
    runtime.unsafe.run(app.connectTool(router.nodeId, ocrTool.nodeId))
    runtime.unsafe.run(app.connectTool(contractProcessor.nodeId, saveTool.nodeId))
    runtime.unsafe.run(app.connectTool(invoiceProcessor.nodeId, saveTool.nodeId))
    runtime.unsafe.run(app.connectTool(reportGenerator.nodeId, saveTool.nodeId))
    runtime.unsafe.run(app.connectContext(router.nodeId, policy.nodeId))
    
    (app, router.nodeId)
  }
  
  def run(app: GraphApp, entryNodeId: String, document: Document): ZIO[Any, Throwable, ExecutionContext] = {
    app.run(
      entryNode = entryNodeId,
      query = s"Process document: ${document.url}",
      strategy = "sequential"
    )
  }
}
```

## Usage

```scala
val runtime = Runtime.default
val store = runtime.unsafe.run(GraphStore.make).getOrThrow
val backend = new AnthropicBackend(apiKey = sys.env("ANTHROPIC_API_KEY"))

Unsafe.unsafe { implicit u =>
  val (app, entryNodeId) = FlagshipWorkflow.buildGraph(store, backend)
  
  // Process a document
  val result = runtime.unsafe.run {
    FlagshipWorkflow.run(app, entryNodeId, Document(
      id = "doc-123",
      docType = "contract",
      value = 150000,
      url = "s3://bucket/contract.pdf"
    ))
  }.getOrThrow
  
  // Inspect results
  println(s"Hops: ${result.hopCount}")
  println(s"Status: ${result.state.status}")
  
  // Print trace
  printTrace(result)
  
  // Explain execution
  val explanation = explainRun(result)
  println(s"Routing decisions: ${explanation.summary.routingDecisionCount}")
  println(s"Tool calls: ${explanation.summary.toolCallCount}")
}
```

## Resume After Approval

```scala
// If paused at approval
if (result.isPaused) {
  val task = result.state.inbox.values.head
  
  println(s"Approval required: ${task.waitingFor}")
  println(s"Assignees: ${task.assignees}")
  
  // After user approves...
  val approved = result.copy(
    state = result.state.copy(
      inbox = result.state.inbox.map { case (id, t) =>
        id -> t.copy(status = "approved", resolvedAt = Some(Instant.now()))
      },
      data = result.state.data + ("approval" -> Map(
        "approved" -> true,
        "assigned_to" -> "legal-team"
      ))
    )
  )
  
  // Resume from the node that was paused (found in the pending pause or inbox)
  val pausedNodeId = result.state.pendingPause.map(_.nodeId).getOrElse(task.nodeId)
  val resumed = runtime.unsafe.run {
    app.getExecutor.resume(pausedNodeId, approved)
  }.getOrThrow
}
```

## Batch Processing

```scala
val documents = List(
  Document("doc-1", "invoice", 1000, "..."),
  Document("doc-2", "contract", 200000, "..."),
  Document("doc-3", "report", 0, "...")
)

val batchExec = new BatchExecutor(store, app.getExecutor, concurrency = 5)

val results = runtime.unsafe.run {
  batchExec.run(
    agentNodeId = entryNodeId,
    items = documents,
    queryFn = (doc: Any) => s"Process: ${doc.asInstanceOf[Document].url}",
    reduceFn = Some((outputs: List[Any]) => outputs.mkString("\n---\n"))
  )
}.getOrThrow

println(s"Processed: ${results.completed}/${results.total}")
println(s"Failed: ${results.failed}")
```

## Monitoring

```scala
app.getExecutor.addEventHook((event, ctx) => {
  event.eventType match {
    case EventType.ApprovalTask =>
      println(s"Approval created: ${event.payload("task_id")}")
      slack.notify(s"Pending approval: ${event.nodeName}")
      
    case EventType.ToolResult if !event.payload.get("success").contains(true) =>
      println(s"Tool failed: ${event.nodeName}")
      metrics.increment("tool_errors", Map("tool" -> event.nodeName))
      
    case EventType.Routing =>
      val mode = event.payload("mode")
      if (mode == "deterministic") {
        println(s"Rule-based route: ${event.payload("intent")}")
      }
      
    case _ =>
  }
})
```
