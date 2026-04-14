# Loki Engine

A Scala-ZIO framework for graph-native agent orchestration. Loki Engine organizes AI workflows as directed graphs where nodes are executable units (agents, tools, contexts) and edges define the flow of execution.

## Core Concepts

### Nodes

Nodes are the fundamental execution units in Loki Engine:

| Type | Description |
|------|-------------|
| `Agent` | Executable node with LLM model and routing table |
| `Tool` | Callable node with JSON Schema I/O |
| `Context` | Passive knowledge/memory chunk with bi-temporal validity |
| `Prompt` | Reusable template with `{{variable}}` interpolation |
| `Schema` | JSON Schema contract for validation |
| `Graph` | Pointer to a sub-graph (hierarchical composition) |
| `Approval` | Human approval / inbox step |
| `Transform` | Pure data-reshaping step — no LLM invocation |
| `Reasoner` | Symbolic Datalog inference — deterministic, verifiable, no LLM |

### Edges

Edges are typed, weighted, and temporally-valid relationships:

| Type | Description |
|------|-------------|
| `HAS_TOOL` | Agent → Tool |
| `HAS_CONTEXT` | Agent → Context |
| `HAS_PROMPT` | Agent → Prompt |
| `DELEGATES_TO` | Agent → Agent (routing/delegation) |
| `PRODUCES` | Agent/Tool → Context (output materialization) |
| `MENTIONS` | Context → Entity |
| `NEXT` | Context → Context (sequence) |
| `COVERS` | Context → Concept tag |
| `SIMILAR_TO` | Semantic similarity |
| `VALIDATES` | Schema → Tool/Context |
| `CONTAINS` | Graph → Node (subgraph membership) |

## Quick Start

```scala
import zio.*
import loki.engine.app.*
import loki.engine.core.store.*
import loki.engine.core.nodes.*
import loki.engine.backend.*

val runtime = Runtime.default

Unsafe.unsafe { implicit u =>
  val store = runtime.unsafe.run(GraphStore.make).getOrThrow

  val agent = createAgent(
    name = "ResearchAgent",
    model = "claude-sonnet-4-6",
    systemPrompt = "You are a helpful research assistant."
  )

  val app = new GraphApp(store, backend = Some(new AnthropicBackend()))
  app.useDefaultTools()                                          // register echo + web_search
  runtime.unsafe.run(app.addAgent("ResearchAgent", agent)).getOrThrow

  val result = runtime.unsafe.run(app.run(agent.nodeId, "What is Scala ZIO?")).getOrThrow

  println(s"Session: ${result.sessionId}")
  println(s"Trace events: ${result.trace.length}")
}
```

## Execution Strategies

Loki Engine supports three execution strategies:

### Sequential (DFS)
Default strategy. Executes nodes depth-first, routing to the next node after each completion.

```scala
executor.run(entryNodeId, query, strategy = "sequential")
```

### Parallel (BFS)
Fans out to all `DELEGATES_TO` targets concurrently.

```scala
executor.run(entryNodeId, query, strategy = "parallel")
```

### Topological (DAG)
Executes nodes in topological order using dependency waves.

```scala
executor.run(entryNodeId, query, strategy = "topological")
```

## Routing

Agents use multiple routing mechanisms (in priority order):

1. **Decision Table** — Declarative decision rules evaluated first
2. **Route Rules** — Deterministic rules based on workflow state or output
3. **Intent Routing** — LLM-inferred routing based on agent output
4. **Default** — Falls back to `__END__` or the `"default"` entry in the routing table

```scala
val agent = createAgent(
  name = "Router",
  routingTable = Map(
    "search" -> "searchNodeId",
    "summarize" -> "summarizeNodeId",
    "default" -> "__END__"
  ),
  routeRules = List(
    RouteRule(
      name = "escalate",
      source = "state",
      path = "ticket.escalated",
      operator = "equals",
      value = true,
      targetNodeId = "managerNodeId",
      priority = 10
    )
  )
)
```

## Neurosymbolic AI — Datalog Reasoning

`ReasonerNode` runs a built-in Datalog engine over ground facts — no LLM, fully deterministic and auditable. Combine it with `AgentNode` for verifiable, explainable AI pipelines.

```scala
// 1. Define a Datalog program
val program = """
  |ancestor(X, Z) :- parent(X, Y), parent(Y, Z).
  |ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
  |safe(X)        :- node(X), !compromised(X).
""".stripMargin

// 2. Attach a ReasonerNode that reads facts from workflow state
val reasoner = ReasonerNode(
  nodeId     = "r1",
  name       = "FamilyReasoner",
  program    = program,
  factSource = FactSource(stateKeys = List("graph_facts")),
  query      = List("ancestor", "safe"),
  outputKey  = "inferred",
  withProof  = true         // include justification traces
)

// 3. Run — output lands in ctx.state.data("inferred")
val ctx = ExecutionContext(
  query = "find ancestors",
  state = WorkflowState(data = Map("graph_facts" -> myFacts))
)
for {
  _      <- store.upsertNode(reasoner)
  result <- executor.run(reasoner.nodeId, "infer", executionContext = Some(ctx))
} yield result.state.data("inferred")
// → Map("facts" -> [...], "fact_count" -> 12, "predicates" -> ["ancestor","safe"], "proofs" -> [...])
```

You can also pull facts directly from knowledge-graph edges:

```scala
val kgReasoner = ReasonerNode(
  name       = "KGReasoner",
  program    = "reachable(X, Z) :- DELEGATES_TO(X, Y), reachable(Y, Z).",
  factSource = FactSource(
    edgeTypes        = List("DELEGATES_TO"),
    includeNodeFacts = true,    // also emit (nodeType, label) for every KG node
    useNodeNames     = true     // use node.name as label instead of nodeId
  ),
  outputKey = "reachability"
)
```

See [docs/neurosymbolic.md](docs/neurosymbolic.md) for the full Datalog language reference.

## Subgraph Composition

`GraphNode` supports hierarchical execution with configurable scoping:

```scala
val subgraph = GraphNode(
  name        = "ProcessingPipeline",
  entryNodeId = "step1",
  exitNodeId  = "step3",
  strategy    = "topological",     // "sequential" | "parallel" | "topological"
  inputKeys   = List("raw_data"),  // pull these state keys into the subgraph query
  inputMap    = Map("data" -> "raw_data"),  // alias "data" to parent's "raw_data" key
  scopeOutputs = true              // true = isolated child context; false = share parent
)
```

Cycle detection and max-depth (16) protection are built in.

## Checkpoint and Resume

Save and restore workflow state across process restarts:

```scala
// Save
val checkpoint = executor.checkpointContext(ctx, "after-step-2")

// Inspect whether a saved context is safe to resume
val readiness = executor.inspectResume(
  ctx,
  staleAfterSeconds = 1800.0,
  requiredOutputs   = List("step1_out", "step2_out")
)
if (readiness.ok) executor.resume("step3", ctx)

// Load and resume from a checkpoint node
executor.resumeFromCheckpoint(checkpoint.nodeId, entryNodeId = "step3")
```

## Multi-Step Planning

Plan a sequence of agents before executing:

```scala
val steps = runtime.unsafe.run {
  executor.plan("Research and summarize AI trends")
}.getOrThrow

// steps: List[RoutingDecision] ordered by the planner
steps.foreach(s => println(s"${s.agentId}: ${s.reason}"))
```

## Context Navigation

The `ContextNavigator` performs graph-native context retrieval with:

- **Direct edge affinity** — Context directly attached via `HAS_CONTEXT`
- **Semantic search** — Vector similarity matching
- **Token budgeting** — Limits context to `maxContextTokens`
- **Per-source limits** — Prevents single source domination
- **Stale fact detection** — Penalises or excludes expired facts

## Semantic Agent Discovery (wRRF)

Use Weighted Reciprocal Rank Fusion to find the best agent for a query:

```scala
import loki.engine.retrieval.WRRF

val queryEmbedding: List[Double] = embedder.embedText("summarize documents")
val candidates = runtime.unsafe.run {
  WRRF.semanticSearch(store, queryEmbedding, topK = 3)
}.getOrThrow

candidates.foreach(r => println(s"${r.agent.name}: ${r.score}"))
```

## Batch Execution

Process items concurrently with checkpointing and resume:

```scala
val batchExec = new BatchExecutor(store, executor, concurrency = 5)

val run = runtime.unsafe.run {
  batchExec.run(
    agentNodeId = agent.nodeId,
    items = documents,
    queryFn = (doc: Any) => s"Summarize: $doc",
    reduceFn = Some((outputs: List[Any]) => outputs.mkString("\n\n"))
  )
}.getOrThrow

println(s"Status: ${run.status}")
println(s"Progress: ${run.progress}")

// Resume after interruption — skips already-completed items
if (run.status == BatchStatus.Partial) {
  val resumed = runtime.unsafe.run {
    batchExec.resume(run, documents, (doc: Any) => s"Summarize: $doc")
  }.getOrThrow
}
```

## Tool Registration

```scala
// Register directly on the executor
executor.registerTool("mymodule.search", searchFn)

// Or auto-register the built-in tools (echo + web_search)
app.useDefaultTools()

val tool = createTool(
  name = "web_search",
  callableRef = "loki.engine.tools.web_search.search",
  inputSchema = Map(
    "type" -> "object",
    "properties" -> Map("query" -> Map("type" -> "string")),
    "required" -> List("query")
  )
)
```

## Observability

### Terminal Trace

```scala
import loki.engine.core.executor.*
printTrace(ctx)
```

### Run Explanation

```scala
import loki.engine.observability.*
val explanation = explainRun(ctx)
println(s"Hops: ${explanation.hopCount}")
println(s"Tool calls: ${explanation.summary.toolCallCount}")
```

### Browser Trace Visualizer

```scala
import loki.engine.viz.TraceServer

// Serve interactive HTML trace at http://localhost:8765
runtime.unsafe.run(TraceServer.serve(ctx, port = 8765)).getOrThrow
```

### OpenTelemetry Export

```scala
import loki.engine.exporters.OTelExporter

// Export to any OTLP HTTP endpoint (Tempo, Honeycomb, Jaeger, Datadog, …)
runtime.unsafe.run {
  OTelExporter.exportTrace(ctx, endpoint = "http://localhost:4318/v1/traces")
}.getOrThrow
```

## Pause / Resume

```scala
// Run until approval is needed
val paused = runtime.unsafe.run(app.run(approvalNode.nodeId, request)).getOrThrow

// Inject the decision and resume
val approved = paused.copy(state = paused.state.copy(
  data = paused.state.data + ("approval" -> Map("approved" -> true)),
  status = "running"
))
val resumed = runtime.unsafe.run(executor.resume(approvalNode.nodeId, approved)).getOrThrow
```

## Building

```bash
cd loki-engine
sbt compile
sbt test
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                           GraphApp                               │
│                       (Builder Facade)                           │
└──────────────────────────────┬───────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────┐
│                          GraphStore                              │
│                   (InMemoryGraphStore)                           │
└──────┬─────────────────────────────────────────┬─────────────────┘
       │                                         │
┌──────▼──────┐  ┌─────────────────┐  ┌─────────▼────────┐
│   Nodes     │  │     Edges       │  │  Vector Search   │
│  (9 types)  │  │  (11 types)     │  │  (cosine sim.)   │
└─────────────┘  └─────────────────┘  └──────────────────┘
       │
┌──────▼──────────────────────────────────────────────────────────┐
│                        GraphExecutor                            │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐    │
│  │ AgentComposer│  │ContextNavigat│  │    LLMBackend      │    │
│  └──────────────┘  └──────────────┘  │ AnthropicBackend   │    │
│                                      │ OpenAIBackend      │    │
│  ┌──────────────┐  ┌──────────────┐  │ (custom)           │    │
│  │BatchExecutor │  │  plan()      │  └────────────────────┘    │
│  └──────────────┘  └──────────────┘                            │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Symbolic: executeReasoner  checkpointContext            │   │
│  │            inspectResume    materialiseOutput            │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
       │                          │
┌──────▼──────────────────┐  ┌───▼────────────────────────────────┐
│  Retrieval              │  │  Symbolic (loki.engine.symbolic)   │
│  WRRF.semanticSearch()  │  │  Program · Rule · Solution         │
│  Embedder               │  │  Datalog parser + semi-naive eval  │
└─────────────────────────┘  └────────────────────────────────────┘
┌────────────────────────────────────────────────────────────────┐
│  Observability: printTrace / explainRun / TraceServer          │
│  Exporters:  OTelExporter.exportTrace()                        │
└────────────────────────────────────────────────────────────────┘
```
