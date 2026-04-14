# Architecture

Loki Engine is built around a graph-based execution model where AI workflows are represented as directed graphs of typed nodes and edges.

## Core Components

```
┌──────────────────────────────────────────────────────────────────────┐
│                            Loki Engine                               │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌───────────────┐    ┌─────────────────┐    ┌───────────────────┐  │
│  │   GraphApp    │───▶│  GraphExecutor  │───▶│   GraphStore      │  │
│  │ (Builder API) │    │  (Orchestrator) │    │ (InMemory / ext.) │  │
│  └───────────────┘    └────────┬────────┘    └─────────┬─────────┘  │
│                                │                        │            │
│        ┌───────────────────────┼────────────────────────┘            │
│        │                      │                                      │
│  ┌─────▼──────┐    ┌──────────▼──────────┐    ┌──────────────────┐  │
│  │   Nodes    │    │   AgentComposer     │    │     Edges        │  │
│  │ (9 types)  │    │ + ContextNavigator  │    │  (11 types)      │  │
│  └────────────┘    └─────────────────────┘    └──────────────────┘  │
│                                                                      │
│  ┌─────────────────────────┐    ┌──────────────────────────────┐    │
│  │      LLM Backends       │    │         Retrieval            │    │
│  │  AnthropicBackend       │    │  Embedder (sentence-xformers)│    │
│  │  OpenAIBackend          │    │  WRRF.semanticSearch()       │    │
│  │  (custom LLMBackend)    │    └──────────────────────────────┘    │
│  └─────────────────────────┘                                        │
│                                                                      │
│  ┌─────────────────────────┐    ┌──────────────────────────────┐    │
│  │   Observability         │    │   Exporters / Viz            │    │
│  │  printTrace             │    │  OTelExporter.exportTrace()  │    │
│  │  explainRun             │    │  TraceServer.serve()         │    │
│  │  extractTraceView       │    └──────────────────────────────┘    │
│  └─────────────────────────┘                                        │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  BatchExecutor (run + resume, bounded concurrency)             │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

## GraphStore

The `GraphStore` is the persistence layer for the graph. It provides:

- **Node operations**: upsert, get, delete, expire, list, vector search
- **Edge operations**: upsert, get, delete, expire, list
- **Graph operations**: neighbors, multi-hop traversal

### InMemoryGraphStore

Default implementation using mutable Maps and cosine similarity for vector search. Suitable for development and testing.

## Nodes

All nodes extend the base `Node` trait with common fields:

```scala
sealed trait Node {
  val nodeId: String
  val nodeType: NodeType
  val name: String
  val description: String
  val embedding: Option[List[Double]]  // For similarity search
  val validAt: Instant                 // When node becomes active
  val invalidAt: Option[Instant]       // When node expires (None = never)
  val attributes: Map[String, Any]     // Extensible metadata
  val groupId: String                  // Multi-tenancy key
}
```

### Node Types

| Type | Description | Execution Behavior |
|------|-------------|-------------------|
| `AgentNode` | LLM-powered agent | Compose → LLM call → tool handling → route |
| `ToolNode` | Callable function | Invoke registered function via `callableRef` |
| `ContextNode` | Knowledge chunk | Return `content` as-is |
| `PromptNode` | Template | Render `{{variable}}` substitutions |
| `SchemaNode` | Validation contract | Passive — used by `VALIDATES` edges |
| `GraphNode` | Subgraph pointer | Recursive execution with strategy, scoping, cycle guard |
| `ApprovalNode` | Human approval | Pause + create `ApprovalTask` in inbox |
| `TransformNode` | Pure data transform | Invoke `callableRef` with collected inputs; no LLM |
| `ReasonerNode` | Symbolic inference | Run Datalog program over facts; fully deterministic |

## Edges

Edges are typed, weighted, directed relationships:

```scala
case class Edge(
  edgeId: String,
  edgeType: EdgeType,
  srcId: String,          // Source node
  dstId: String,          // Target node
  weight: Double,         // Relationship strength (0.0–1.0)
  description: String,
  validAt: Instant,
  invalidAt: Option[Instant],
  requiredRoles: List[String]
)
```

### Edge Composition

The key architectural insight: **composition is traversal**.

```
Adding a tool to an agent  =  upsertEdge(HAS_TOOL, agent, tool)
Removing a tool            =  expireEdge(HAS_TOOL, agent, tool)
```

No code changes required to modify agent composition at runtime.

## GraphExecutor

The orchestrator that traverses the graph:

```scala
class GraphExecutor(
  store: GraphStore,
  backend: LLMBackend,
  composer: AgentComposer,
  routerModel: String = "claude-haiku-4-5-20251001"
)
```

### Execution Flow

```
run(entryNodeId, query, strategy)
  └──▶ runSequential | runParallel | runTopological
       └──▶ executeNode(node, ctx)
            ├── AgentNode   ▶ executeAgent
            │    ├── AgentComposer.compose         (tools + context + prompt)
            │    ├── ContextNavigator.navigate     (scored context selection)
            │    ├── backend.chat(...)             (LLM call)
            │    ├── runAgentLoop                  (tool-use loop until end_turn)
            │    └── routeNode                     (decision table → route rules → LLM intent)
            ├── ToolNode     ▶ executeTool            (invoke registered fn)
            ├── ContextNode  ▶ return content
            ├── PromptNode   ▶ render template
            ├── GraphNode    ▶ executeSubgraph        (cycle guard → dispatchStrategy)
            ├── TransformNode ▶ executeTransform      (collect inputs → callableRef)
            ├── ReasonerNode ▶ executeReasoner        (gatherFacts → Program.solve → filter)
            └── ApprovalNode ▶ pause + inbox task
```

### AgentComposer

Discovers agent composition via graph traversal:

1. **Tools**: `HAS_TOOL` edges → ToolNodes (sorted by edge weight)
2. **Context**: `ContextNavigator.navigate` → scored ContextNodes
3. **Prompt**: `HAS_PROMPT` edge → PromptNode (first match)
4. **Delegates**: `DELEGATES_TO` edges → AgentNodes

### ContextNavigator

Graph-native context retrieval with token budgeting:

1. **Direct edges**: `HAS_CONTEXT` → scored by `semantic × affinity × pathDecay`
2. **Semantic search**: Vector similarity via embedder
3. **Scoring factors**: path decay, priority, recency, provenance, stale-fact penalty
4. **Token budget**: caps total context at `maxContextTokens`
5. **Per-source limit**: prevents one source from dominating

### WRRF — Semantic Agent Discovery

`WRRF.semanticSearch()` finds the best agent for a query using Weighted Reciprocal Rank Fusion:

1. Search `ToolNode`s by embedding similarity → walk upstream `HAS_TOOL` edges → parent agents (tool-based path)
2. Search `AgentNode`s directly by embedding similarity (agent-based path)
3. Fuse both ranked lists: `score = wTool × (1 / (k + rank_tools)) + wAgent × (1 / (k + rank_agents))`
4. Add optional tag bonus

## Routing

Multi-stage routing evaluated in priority order:

```
1. DecisionTable  (if configured on AgentNode)
   └── Find first DecisionRule whose conditions all pass → return targetNodeId

2. RouteRules  (sorted by priority descending)
   └── Find first rule whose condition matches (state or result) → return targetNodeId

3. Intent inference  (keyword match → routing table lookup)
   └── Extract intent from LLM output text
   └── Lookup intent in routingTable

4. Default  →  "__END__"
```

Routing mode is recorded in the `routing` trace event as `decision_table`, `deterministic`, or `llm`.

## Symbolic Subsystem (`loki.engine.symbolic`)

`ReasonerNode` delegates to a self-contained Datalog engine in `symbolic/Datalog.scala`. It is entirely independent of ZIO and the rest of the engine.

```
gatherReasonerFacts(node, ctx)
  ├── state.data[stateKeys]      (key-value facts from workflow state)
  ├── store.listEdges(edgeTypes) (KG edges → (pred, src, dst) ground facts)
  └── store.listNodes()          (optionally: (nodeType, label) facts)

Program.solve(facts, withProof)
  ├── normaliseFact(x) per input item
  ├── stratify(rules)            (compute safe evaluation order)
  └── for each stratum:
        semi-naive fixpoint loop
          matchBody(rule.body, db) → unify + recurse
          groundAtom(head, binding)
          (optionally record justification)

Solution → filter by query predicates → emit as Map
```

### Datalog semantics

- **Forward chaining** — semi-naive bottom-up evaluation
- **Stratified negation** — safe negation-as-failure; recursive negation raises `StratificationError`
- **Safety checking** — every head variable must appear in a positive body literal
- **Proof traces** — `withProof=true` records `(ruleName, supportFacts)` per derived fact

## LLM Backends

Both built-in backends use `java.net.http.HttpClient` (JDK 11+) — no additional HTTP library dependency.

- **`AnthropicBackend`** — Anthropic Messages API; handles `tool_use` content blocks and multi-turn tool loops; reads `ANTHROPIC_API_KEY`
- **`OpenAIBackend`** — OpenAI-compatible; converts Anthropic message/tool format ↔ OpenAI; reads `OPENAI_API_KEY` + `OPENAI_BASE_URL`
- **Custom** — Implement the `LLMBackend` trait

## Concurrency

Loki Engine uses ZIO for concurrent execution:

- **Sequential**: `flatMap` chaining of ZIO effects
- **Parallel**: `ZIO.foreachPar` for fan-out to delegates
- **Batch**: `ZIO.foreachPar(...).withParallelism(concurrency)` with configurable limit

## Observability

### Trace Events

Structured, typed events form a tree via `parentEventId`:

```
hop (root)
  └── agent_start/end
        └── tool_call/result
        └── context_inject (sibling)
        └── routing        (sibling)
```

### Output Options

| Method | What |
|--------|------|
| `printTrace(ctx)` | Plain-text terminal tree |
| `TraceServer.serve(ctx)` | Interactive browser HTML |
| `OTelExporter.exportTrace(ctx, endpoint)` | OTLP HTTP/JSON to Tempo, Jaeger, Honeycomb, … |

## Extensibility

### Custom Backend

```scala
class MyBackend extends LLMBackend {
  def chat(model: String, system: String, messages: List[Map[String, Any]],
           tools: List[Map[String, Any]], maxTokens: Int): Task[LLMResponse] = {
    // Call your LLM; return LLMResponse(text, toolCalls, stopReason)
  }

  def extendMessages(messages: List[Map[String, Any]], response: LLMResponse,
                     toolResults: List[ToolResult]): List[Map[String, Any]] = {
    // Append assistant turn + tool results in your backend's native format
  }
}
```

### Custom Embedder

```scala
class MyEmbedder extends loki.engine.backend.Embedder {
  def embedText(text: String): Task[List[Double]] = {
    // Return a normalised embedding vector
  }
}
```
