# Repository Map

## Directory Structure

```
loki-engine/
├── build.sbt                      # Scala build configuration
├── README.md                      # Main documentation
├── docs/                          # Detailed documentation
│   ├── start-here.md             # Getting started
│   ├── architecture.md           # System architecture
│   ├── first-graph.md            # First workflow tutorial
│   ├── workflow-patterns.md      # Common patterns
│   ├── observability.md          # Trace and debugging
│   ├── advanced-configuration.md # Customization
│   ├── long-running-workflows.md # Pause/resume, approvals
│   ├── graph-state-management.md # State handling
│   ├── batch-execution.md        # Batch processing
│   ├── choose-backend.md         # LLM backends
│   ├── core-runtime-maturity.md  # Reliability
│   ├── explainable-systems.md    # Debugging
│   ├── neurosymbolic.md          # Datalog engine + ReasonerNode guide
│   ├── why-behavior-changed.md   # Migration guide
│   └── API_REFERENCE.md          # Complete API docs
└── src/main/scala/loki/engine/
    ├── package.scala              # Package object with public exports
    ├── examples/
    │   └── Example.scala          # Basic usage example
    ├── core/
    │   ├── nodes/
    │   │   ├── Types.scala        # Enums: NodeType, EdgeType, EventType, BatchStatus
    │   │   └── Node.scala         # All node case classes + AnyNode.fromMap
    │   ├── edges/
    │   │   └── Edge.scala         # Edge case class + factory methods
    │   ├── store/
    │   │   └── GraphStore.scala   # GraphStore trait + InMemoryGraphStore
    │   └── executor/
    │       ├── Executor.scala     # GraphExecutor, AgentComposer, ContextNavigator,
    │       │                      #   ExecutionContext, ResumeReadiness, TraceEvent,
    │       │                      #   RoutingDecision, JsonUtil, printTrace,
    │       │                      #   getRuntimeNodes, cleanupSession
    │       └── BatchExecutor.scala # BatchExecutor (run + resume), BatchRun, BatchItemResult
    ├── backend/
    │   ├── LLMBackend.scala       # LLMBackend trait, Embedder trait, LLMResponse, ToolCall
    │   ├── AnthropicBackend.scala # Anthropic Messages API implementation
    │   ├── OpenAIBackend.scala    # OpenAI-compatible implementation (Ollama, vLLM, etc.)
    │   ├── JsonScan.scala         # Shared balanced-brace JSON scanning (tool-call extraction)
    │   └── ToolCallRecovery.scala # Detect/recover tool calls a local model wrote as text
    ├── retrieval/
    │   ├── Embedder.scala         # Embedder class (sentence-transformers bridge)
    │   └── WRRF.scala             # WRRF.semanticSearch() — Weighted RRF agent discovery
    ├── exporters/
    │   └── OTelExporter.scala     # OTelExporter.exportTrace() — OTLP HTTP/JSON export
    ├── viz/
    │   └── TraceServer.scala      # TraceServer.serve() — ZIO HTTP browser trace visualizer
    ├── app/
    │   └── GraphApp.scala         # GraphApp builder facade + createAgent/Tool/Context/Prompt/Executor
    ├── observability/
    │   └── Observability.scala    # explainRun, extractTraceView, RunExplanation
    ├── symbolic/
    │   └── Datalog.scala          # Datalog engine: Program, Rule, Solution, DatalogParser,
    │                              #   fact/factToDict/normaliseFact/parseProgram/ruleFromObj
    └── tools/
        └── ToolRegistry.scala     # ToolRegistry, DefaultTools (echo + web_search), CodeExecTool
```

## Package Organization

### `loki.engine`

Top-level package. The `package.scala` object re-exports the most common symbols:

```scala
import loki.engine.*                // createAgent, createTool, createContext, createPrompt, createExecutor, END_NODE
import loki.engine.app.*            // GraphApp, createAgent, createTool, …
import loki.engine.core.nodes.*     // AgentNode, ToolNode, ContextNode, …
import loki.engine.core.edges.*     // Edge, EdgeType
import loki.engine.core.store.*     // GraphStore, InMemoryGraphStore
import loki.engine.core.executor.*  // GraphExecutor, ExecutionContext, TraceEvent, printTrace, …
import loki.engine.backend.*        // LLMBackend, AnthropicBackend, OpenAIBackend, LLMResponse, …
import loki.engine.retrieval.*      // Embedder, WRRF, RetrievalResult
import loki.engine.observability.*  // explainRun, extractTraceView, RunExplanation
import loki.engine.exporters.*      // OTelExporter
import loki.engine.viz.*            // TraceServer
import loki.engine.symbolic.*        // Program, Rule, Solution, fact, parseProgram, ruleFromObj, …
import loki.engine.tools.*          // ToolRegistry, DefaultTools, EchoTool, WebSearchTool
```

### `loki.engine.core`

Core domain models and execution engine:

- **nodes/** — 9 node type definitions and `AnyNode.fromMap` deserializer
- **edges/** — Edge type definitions and factory methods
- **store/** — Graph persistence (trait + in-memory implementation)
- **executor/** — Orchestration engine, batch executor, trace types, checkpoint helpers

### `loki.engine.backend`

LLM integration:

- `LLMBackend` — Trait: `chat()` + `extendMessages()`
- `AnthropicBackend` — Anthropic Messages API via Java 11 HttpClient; reads `ANTHROPIC_API_KEY`
- `OpenAIBackend` — OpenAI-compatible endpoint; reads `OPENAI_API_KEY` + `OPENAI_BASE_URL`
- `Embedder` — Trait: `embedText(text)`

### `loki.engine.retrieval`

Semantic search:

- `Embedder` — Wraps sentence-transformers via JVM reflection; `embedText`, `embedNode`, `embedAll`
- `WRRF` — `semanticSearch()` via Weighted Reciprocal Rank Fusion (tool-path + agent-path fusion)

### `loki.engine.exporters`

Trace export:

- `OTelExporter` — `exportTrace(ctx, endpoint, serviceName)` sends OTLP HTTP/JSON to any backend

### `loki.engine.viz`

Browser visualizer:

- `TraceServer` — `serve(ctx, port)` / `serveBackground(ctx, port)` — ZIO HTTP server with interactive HTML trace tree, `/data` JSON endpoint, `/explain` explanation endpoint

### `loki.engine.app`

High-level builder API:

- `GraphApp` — Fluent builder: `addAgent`, `addTool`, `addContext`, `addPrompt`, `useDefaultTools`, `connectTool`, `connectContext`, `delegate`, `run`
- Factory functions: `createAgent`, `createTool`, `createContext`, `createPrompt`, `createExecutor`

### `loki.engine.observability`

Trace analysis:

- `explainRun` — Produce `RunExplanation` with per-hop, routing, context, and tool-call summaries
- `extractTraceView` — Categorize `List[TraceEvent]` into typed buckets
- `printTrace` — Console output (defined in `Executor.scala`, exported via `core.executor`)

### `loki.engine.symbolic`

Datalog symbolic reasoning engine — pure Scala 3, no ZIO dependency:

- `Program` — Compiles rules, stratifies, and solves via semi-naive forward chaining
- `Rule / Atom / BodyLiteral / Comparison` — AST types
- `Solution` — Query results with optional proof traces
- `DatalogParser` — Recursive-descent parser; supports uppercase-variable and `?X` conventions
- `fact / factToDict / normaliseFact / parseProgram / ruleFromObj` — Package-level helpers

### `loki.engine.tools`

Built-in tools and registry:

- `DefaultToolRegistry` / `ToolRegistry` — `register`, `get`, `attach`
- `DefaultTools` — Pre-registered: `loki.engine.tools.echo.echo`, `loki.engine.tools.web_search.search`
- `EchoTool`, `WebSearchTool`, `CodeExecTool` — Stub implementations

## Key Files

| File | Purpose |
|------|---------|
| `Executor.scala` | Main orchestration engine, trace types, routing, context navigation, checkpoint helpers |
| `BatchExecutor.scala` | Concurrent batch processing with checkpoint/resume |
| `Node.scala` | All 9 node type definitions (incl. `TransformNode`, `ReasonerNode`, `FactSource`) |
| `Datalog.scala` | Self-contained Datalog engine (parser + semi-naive fixpoint + proof traces) |
| `GraphStore.scala` | Persistence interface + in-memory implementation |
| `GraphApp.scala` | Builder facade + factory helpers |
| `LLMBackend.scala` | LLM abstraction trait + data types |
| `AnthropicBackend.scala` | Anthropic HTTP implementation |
| `OpenAIBackend.scala` | OpenAI-compatible HTTP implementation |
| `JsonScan.scala` | Shared balanced-brace JSON scanning for tool-call extraction |
| `ToolCallRecovery.scala` | Hermes/Llama tool-call markup recovery, mangled-argument detection |
| `WRRF.scala` | Weighted Reciprocal Rank Fusion retrieval |
| `OTelExporter.scala` | OpenTelemetry trace export |
| `TraceServer.scala` | Browser trace visualizer |

## Dependencies

```scala
// build.sbt
val zioVersion = "2.1.4"
val zioSchemaVersion = "1.0.0"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"                 % zioVersion,
  "dev.zio" %% "zio-streams"         % zioVersion,
  "dev.zio" %% "zio-schema"          % zioSchemaVersion,
  "dev.zio" %% "zio-schema-json"     % zioSchemaVersion,
  "dev.zio" %% "zio-http"            % "3.0.1",        // TraceServer
  "io.netty" % "netty-handler"       % "4.1.115.Final",
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0"
)
// No external HTTP client library needed — LLM backends use java.net.http.HttpClient (JDK 11+)
```
