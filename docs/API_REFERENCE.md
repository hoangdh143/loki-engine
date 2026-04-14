# API Reference

Complete API reference for Loki Engine.

## Core Classes

### Node Types

#### AgentNode

```scala
case class AgentNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Agent,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  model: String = "claude-sonnet-4-6",
  systemPrompt: String = "",
  maxIterations: Int = 10,
  maxTokens: Int = 8096,
  routingTable: Map[String, String] = Map("default" -> "__END__"),
  routeRules: List[RouteRule] = Nil,
  decisionTable: Option[DecisionTable] = None,
  executionPolicy: ExecutionPolicy = ExecutionPolicy(),
  stateSchema: Map[String, Any] = Map.empty,
  constraintRules: List[ConstraintRule] = Nil,
  pauseBefore: Boolean = false,
  pauseAfter: Boolean = false,
  waitForInput: Option[String] = None
) extends Node
```

#### ToolNode

```scala
case class ToolNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Tool,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  inputSchema: Map[String, Any] = Map.empty,
  outputSchema: Map[String, Any] = Map.empty,
  callableRef: String = "",
  isAsync: Boolean = true,
  tags: List[String] = Nil,
  executionPolicy: ExecutionPolicy = ExecutionPolicy()
) extends Node {
  def toToolSchema: Map[String, Any]  // Returns Anthropic tool schema format
}
```

#### ContextNode

```scala
case class ContextNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Context,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  content: String = "",
  contentType: String = "text",
  source: String = "",
  tokenCount: Int = 0,
  tags: List[String] = Nil,
  priority: Int = 0,
  factValidAt: Option[Instant] = None,    // Real-world fact validity start
  factInvalidAt: Option[Instant] = None   // Real-world fact validity end
) extends Node {
  def isFactValid: Boolean  // True if fact is currently valid in the real world
}
```

#### PromptNode

```scala
case class PromptNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Prompt,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  template: String = "",
  variables: List[String] = Nil
) extends Node {
  def render(vars: Map[String, Any]): String  // Replaces {{key}} with vars(key)
}
```

#### ApprovalNode

```scala
case class ApprovalNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Approval,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  instructions: String = "",
  assignees: List[String] = Nil,
  assignmentMode: String = "any",
  slaSeconds: Option[Int] = None,
  escalationTarget: String = "",
  inputKey: String = "approval",
  approvedTargetId: String = "__END__",
  rejectedTargetId: String = "__END__",
  requireAssignment: Boolean = false
) extends Node
```

#### GraphNode

```scala
case class GraphNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Graph,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  entryNodeId: String = "",
  exitNodeId: String = "",
  strategy: String = "sequential",     // "sequential" | "parallel" | "topological"
  inputKeys: List[String] = Nil,        // state/output keys to pull into subgraph query
  inputMap: Map[String, String] = Map.empty, // alias -> source key for sub-state injection
  scopeOutputs: Boolean = true,         // true = child context; false = share parent
  executionPolicy: ExecutionPolicy = ExecutionPolicy()
) extends Node
```

#### TransformNode

Pure data-reshaping step — no LLM invocation. Receives a `Map[String, Any]` of collected inputs and returns any value.

```scala
case class TransformNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Transform,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  callableRef: String = "",         // registered fn: Map[String, Any] => Any
  inputKeys: List[String] = Nil,    // node IDs or state keys to collect as inputs
  outputKey: String = "",           // if set, also writes result to state.data[outputKey]
  isAsync: Boolean = true,
  executionPolicy: ExecutionPolicy = ExecutionPolicy()
) extends Node
```

#### FactSource

Declares where a `ReasonerNode` collects its ground facts.

```scala
case class FactSource(
  stateKeys: List[String] = Nil,        // state.data keys to read (empty = use last output)
  edgeTypes: List[String] = Nil,        // KG edge types to materialise as (pred, src, dst) facts
  includeNodeFacts: Boolean = false,    // also emit (nodeType, label) for every node in the KG
  useNodeNames: Boolean = true          // use node.name as label; false = use nodeId
)
```

#### ReasonerNode

Symbolic inference step backed by the built-in Datalog engine. Runs deterministic, sound rules over ground facts — no LLM invoked.

```scala
case class ReasonerNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Reasoner,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  program: String = "",                         // Datalog program source
  rules: List[Map[String, Any]] = Nil,          // alternative: list of rule objects
  factSource: FactSource = FactSource(),
  query: List[String] = Nil,                    // filter output to these predicates (empty = all)
  emitDerivedOnly: Boolean = true,              // false = also emit input facts
  withProof: Boolean = false,                   // include justification traces
  outputKey: String = "inferred",               // writes result to state.data[outputKey]
  failOnEmpty: Boolean = false,                 // throw if no facts match query
  executionPolicy: ExecutionPolicy = ExecutionPolicy()
) extends Node
```

**Output shape** (`ctx.outputs[nodeId]` and `state.data[outputKey]`):

```scala
Map(
  "facts"       -> List[Map[String, Any]],  // [{predicate, args}, ...]
  "fact_count"  -> Int,
  "input_count" -> Int,
  "predicates"  -> List[String],            // distinct predicates in output
  "proofs"      -> List[Map]                // only when withProof=true
)
```

#### AnyNode deserialization

```scala
object AnyNode {
  def fromMap(data: Map[String, Any]): AnyNode
}
```

---

### GraphStore

```scala
trait GraphStore {
  def upsertNode(node: Node): UIO[Unit]
  def upsertEdge(edge: Edge): UIO[Unit]
  def getNode(nodeId: String): UIO[Option[AnyNode]]
  def getEdge(edgeId: String): UIO[Option[Edge]]
  def getEdges(nodeId: String, edgeType: Option[EdgeType] = None,
               direction: String = "out", onlyValid: Boolean = true): UIO[List[Edge]]
  def neighbors(nodeId: String, edgeType: Option[EdgeType] = None,
                depth: Int = 1, onlyValid: Boolean = true): UIO[List[AnyNode]]
  def vectorSearch(embedding: List[Double], nodeTypes: Option[List[NodeType]] = None,
                   topK: Int = 10, onlyValid: Boolean = true): UIO[List[(AnyNode, Double)]]
  def deleteNode(nodeId: String): UIO[Unit]
  def deleteEdge(edgeId: String): UIO[Unit]
  def listNodes(nodeType: Option[NodeType] = None, groupId: Option[String] = None,
                onlyValid: Boolean = true): UIO[List[AnyNode]]
  def listEdges(edgeType: Option[EdgeType] = None, groupId: Option[String] = None,
                onlyValid: Boolean = true): UIO[List[Edge]]
  def expireNode(nodeId: String): UIO[Unit]
  def expireEdge(edgeId: String): UIO[Unit]
  def attachContext(agentId: String, ctxId: String,
                    weight: Option[Double] = None,
                    attrs: Map[String, Any] = Map.empty): UIO[Edge]
  def attachTool(agentId: String, toolId: String,
                 weight: Option[Double] = None,
                 attrs: Map[String, Any] = Map.empty): UIO[Edge]
}

object GraphStore {
  def make: UIO[InMemoryGraphStore]
}
```

---

### GraphExecutor

```scala
class GraphExecutor(
  val store: GraphStore,
  val backend: LLMBackend,
  val composer: AgentComposer,
  val routerModel: String = "claude-haiku-4-5-20251001"
) {
  def registerTool(callableRef: String, fn: Any => Any): Unit
  def addEventHook(fn: (TraceEvent, ExecutionContext) => Any): Unit

  def run(
    entryNodeId: String,
    query: Any,
    strategy: String = "sequential",
    maxHops: Int = 20,
    extraMessages: Option[List[Map[String, Any]]] = None,
    executionContext: Option[ExecutionContext] = None,
    state: Option[Map[String, Any]] = None,
    allowedTools: Option[List[String]] = None,
    options: Option[ExecutionOptions] = None
  ): ZIO[Any, Throwable, ExecutionContext]

  def resume(
    entryNodeId: String,
    ctx: ExecutionContext,
    query: Option[Any] = None,
    strategy: String = "sequential"
  ): ZIO[Any, Throwable, ExecutionContext]

  def route(
    query: String,
    candidates: Option[List[AgentNode]] = None
  ): ZIO[Any, Throwable, RoutingDecision]

  def plan(
    query: String,
    candidates: Option[List[AgentNode]] = None
  ): ZIO[Any, Throwable, List[RoutingDecision]]

  def execute(
    agentId: String,
    query: Any,
    routing: Option[RoutingDecision] = None
  ): ZIO[Any, Throwable, AgentResult]

  // Symbolic reasoning
  def inspectResume(
    ctx: ExecutionContext,
    staleAfterSeconds: Double = 1200.0,
    requiredOutputs: List[String] = Nil,
    now: Option[java.time.Instant] = None
  ): ResumeReadiness

  def checkpointContext(
    ctx: ExecutionContext,
    name: String = "Execution checkpoint"
  ): ZIO[Any, Throwable, ContextNode]

  def loadCheckpoint(checkpointNodeId: String): ZIO[Any, Throwable, ExecutionContext]

  def resumeFromCheckpoint(
    checkpointNodeId: String,
    entryNodeId: String,
    query: Option[Any] = None,
    strategy: String = "sequential"
  ): ZIO[Any, Throwable, ExecutionContext]

  def materialiseOutput(
    sourceNode: AnyNode,
    output: Any,
    ctx: ExecutionContext
  ): ZIO[Any, Throwable, ContextNode]
}
```

---

### ExecutionContext

```scala
case class ExecutionContext(
  query: Any,
  sessionId: String = UUID.randomUUID().toString,
  outputs: Map[String, Any] = Map.empty,
  trace: List[TraceEvent] = Nil,
  maxHops: Int = 20,
  hopCount: Int = 0,
  extraMessages: List[Map[String, Any]] = Nil,
  state: WorkflowState = WorkflowState(),
  allowedTools: Option[Set[String]] = None,
  activeSubgraphs: List[String] = Nil   // cycle guard for nested GraphNodes
) {
  def isPaused: Boolean
  def snapshot: Map[String, Any]  // Serializable snapshot of the full context
  def withTrace(event: TraceEvent): ExecutionContext
}

object ExecutionContext {
  def fromSnapshot(data: Map[String, Any]): ExecutionContext
  // Reconstructs a full ExecutionContext from a snapshot map (e.g. loaded from a ContextNode).
}
```

#### ResumeReadiness

```scala
case class ResumeReadiness(
  status: String,
  isPaused: Boolean,
  isStale: Boolean,
  lastEventAt: Option[java.time.Instant],
  secondsSinceLastEvent: Option[Double],
  available: List[String],     // required outputs that are present
  unrecoverable: List[String]  // required outputs that are missing
) {
  def ok: Boolean  // true iff !isStale && unrecoverable.isEmpty
}
```

---

### TraceEvent

```scala
case class TraceEvent(
  eventType: EventType,
  sessionId: String,
  nodeId: String,
  nodeName: String,
  timestamp: Instant = Instant.now(),
  payload: Map[String, Any] = Map.empty,
  eventId: String = UUID.randomUUID().toString,
  parentEventId: Option[String] = None,
  durationMs: Option[Long] = None
)
```

---

## Builder Functions

### createAgent

```scala
def createAgent(
  name: String,
  description: String = "",
  model: String = "claude-sonnet-4-6",
  systemPrompt: String = "",
  routingTable: Option[Map[String, String]] = None,
  maxIterations: Int = 10,
  maxTokens: Int = 8096
): AgentNode
```

### createTool

```scala
def createTool(
  name: String,
  callableRef: String,
  description: String = "",
  inputSchema: Map[String, Any] = Map.empty,
  outputSchema: Map[String, Any] = Map.empty,
  isAsync: Boolean = true
): ToolNode
```

### createContext

```scala
def createContext(
  name: String,
  content: String,
  description: String = ""
): ContextNode
```

### createPrompt

```scala
def createPrompt(
  name: String,
  template: String,
  description: String = ""
): PromptNode
```

### createExecutor

```scala
def createExecutor(
  store: GraphStore = InMemoryGraphStore(),
  backend: LLMBackend = new DefaultLLMBackend,
  routerModel: String = "claude-haiku-4-5-20251001"
): GraphExecutor
```

---

## GraphApp

```scala
class GraphApp(
  store: GraphStore = InMemoryGraphStore(),
  executor: Option[GraphExecutor] = None,
  backend: Option[LLMBackend] = None
) {
  def getExecutor: GraphExecutor

  def addAgent(name: String, agent: AgentNode): UIO[AgentNode]
  def addTool(name: String, tool: ToolNode, fn: Option[Any => Any] = None): UIO[ToolNode]
  def addContext(name: String, ctx: ContextNode): UIO[ContextNode]
  def addPrompt(name: String, prompt: PromptNode): UIO[PromptNode]

  def registerTool(callableRef: String, fn: Any => Any): Unit
  def useDefaultTools(): Unit   // Registers echo + web_search from DefaultTools

  def connectTool(agent: NodeRef, tool: NodeRef, weight: Option[Double] = None): UIO[Edge]
  def connectContext(agent: NodeRef, ctx: NodeRef, weight: Option[Double] = None): UIO[Edge]
  def connectPrompt(agent: NodeRef, prompt: NodeRef): UIO[Edge]
  def delegate(srcAgent: NodeRef, dstAgent: NodeRef): UIO[Edge]

  def run(
    entryNode: NodeRef,
    query: Any,
    strategy: String = "sequential"
  ): ZIO[Any, Throwable, ExecutionContext]
}
```

---

## BatchExecutor

```scala
class BatchExecutor(
  store: GraphStore,
  executor: GraphExecutor,
  concurrency: Int = 10          // Max parallel items via ZIO .withParallelism
) {
  def run(
    agentNodeId: String,
    items: List[Any],
    queryFn: Any => String,
    contextFn: Option[Any => String] = None,    // Injected as an extra message per item
    reduceFn: Option[List[Any] => Any] = None,  // Map-reduce aggregation
    onProgress: Option[BatchRun => Any] = None,
    checkpoint: Boolean = true,
    strategy: String = "sequential"
  ): ZIO[Any, Throwable, BatchRun]

  def resume(
    existingRun: BatchRun,       // Pass the BatchRun from a previous run() call
    items: List[Any],
    queryFn: Any => String,
    contextFn: Option[Any => String] = None,
    reduceFn: Option[List[Any] => Any] = None,
    onProgress: Option[BatchRun => Any] = None
  ): ZIO[Any, Throwable, BatchRun]
}

case class BatchRun(
  runId: String,
  agentId: String,
  total: Int,
  results: Map[String, BatchItemResult],
  reducedOutput: Any,
  startedAt: Instant,
  endedAt: Option[Instant]
) {
  def completed: Int
  def failed: Int
  def pending: Int
  def running: Int
  def status: BatchStatus
  def progress: Double           // 0.0 – 1.0
  def successfulOutputs: List[Any]
}

case class BatchItemResult(
  itemId: String,
  status: BatchStatus,
  output: Any,
  error: Option[String],
  startedAt: Option[Instant],
  endedAt: Option[Instant]
) {
  def durationSeconds: Option[Double]
}
```

---

## LLM Backends

### LLMBackend (trait)

```scala
trait LLMBackend {
  def chat(
    model: String,
    system: String,
    messages: List[Map[String, Any]],
    tools: List[Map[String, Any]],
    maxTokens: Int = 8096
  ): Task[LLMResponse]

  def extendMessages(
    messages: List[Map[String, Any]],
    response: LLMResponse,
    toolResults: List[ToolResult]
  ): List[Map[String, Any]]
}
```

### AnthropicBackend

```scala
class AnthropicBackend(
  apiKey: String = sys.env.getOrElse("ANTHROPIC_API_KEY", ""),
  baseUrl: String = "https://api.anthropic.com/v1/messages",
  anthropicVersion: String = "2023-06-01"
) extends LLMBackend
```

Uses `java.net.http.HttpClient` (JDK 11+). No additional dependencies required.

### OpenAIBackend

```scala
class OpenAIBackend(
  baseUrl: String = sys.env.getOrElse("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions"),
  apiKey: String = sys.env.getOrElse("OPENAI_API_KEY", "")
) extends LLMBackend
```

Compatible with Ollama, mlx-lm, LM Studio, vLLM, Together AI, and OpenAI.

### LLMResponse / ToolCall / ToolResult

```scala
case class LLMResponse(
  text: String,
  toolCalls: List[ToolCall],
  // "end_turn" | "tool_use" | "max_tokens" | "unparsed_tool_call"
  stopReason: String,
  raw: Any = null,      // Raw response body (String)
  reasoning: String = "" // Chain-of-thought from a side channel
                         // (OpenAI `reasoning_content`, Anthropic `thinking`)
)

case class ToolCall(id: String, name: String, input: Map[String, Any])
case class ToolResult(toolCallId: String, content: String)
```

`unparsed_tool_call` means the model wrote a tool call the API layer never
turned into a structured call, *and* it could not be recovered from the text
either — the turn did no work and is not a final answer. The executor answers
it with a bounded re-prompt rather than accepting the markup as the result. See
[Tolerating what a local model actually does](advanced-configuration.md#tolerating-what-a-local-model-actually-does).

---

## Retrieval

### Embedder

```scala
class Embedder(model: String = "all-MiniLM-L6-v2") {
  def embedText(text: String): Task[List[Double]]
  def embedNode(store: GraphStore, node: AnyNode): Task[AnyNode]
  def embedAll(store: GraphStore, nodeTypes: Option[List[NodeType]] = None,
               skipExisting: Boolean = true): Task[Int]
}
```

### WRRF — Weighted Reciprocal Rank Fusion

```scala
object WRRF {
  def semanticSearch(
    store: GraphStore,
    queryEmbedding: List[Double],
    topK: Int = 5,
    topKTools: Int = 20,
    topKAgents: Int = 10,
    wTool: Double = 0.7,   // Weight for tool-based hits
    wAgent: Double = 0.3,  // Weight for direct agent hits
    k: Int = 60,           // RRF constant
    queryTags: List[String] = Nil,
    tagWeight: Double = 0.1
  ): URIO[Any, List[RetrievalResult]]
}

case class RetrievalResult(
  agent: AgentNode,
  score: Double,
  viaTools: List[ToolScore] = Nil,
  toolScores: Map[String, Double] = Map.empty
)

case class ToolScore(toolId: String, toolName: String, score: Double, rank: Int)
```

---

## Observability

### printTrace

```scala
def printTrace(ctx: ExecutionContext, width: Int = 72): Unit
```

Renders a hierarchical plain-text trace tree to stdout.

### explainRun

```scala
def explainRun(ctx: ExecutionContext): RunExplanation

case class RunExplanation(
  sessionId: String,
  query: Any,
  hopCount: Int,
  paused: Boolean,
  hops: List[RunHopExplanation],
  routing: List[RunRoutingExplanation],
  context: List[RunContextExplanation],
  toolCalls: List[RunToolCallExplanation],
  pauses: List[RunPauseExplanation],
  summary: RunSummary
)

case class RunSummary(
  routingDecisionCount: Int,
  toolCallCount: Int,
  contextInjectionCount: Int,
  pauseEventCount: Int
)
```

### extractTraceView

```scala
def extractTraceView(events: List[TraceEvent]): TraceView

case class TraceView(
  hops: List[TraceEvent],
  agentStarts: List[TraceEvent],
  agentEnds: List[TraceEvent],
  toolCalls: List[TraceEvent],
  toolResults: List[TraceEvent],
  contextInject: List[TraceEvent],
  routing: List[TraceEvent],
  pauses: List[TraceEvent]
)
```

### getRuntimeNodes

```scala
def getRuntimeNodes(
  store: GraphStore,
  sessionId: Option[String] = None,
  onlyValid: Boolean = true
): URIO[Any, List[ContextNode]]
```

Returns `ContextNode`s tagged with `origin: "runtime"` (materialized during execution).

### cleanupSession

```scala
def cleanupSession(
  store: GraphStore,
  sessionId: String,
  hard: Boolean = false   // false = soft expire, true = hard delete
): URIO[Any, Int]
```

---

## OpenTelemetry Export

```scala
object OTelExporter {
  def exportTrace(
    ctx: ExecutionContext,
    endpoint: String = sys.env.getOrElse("OTEL_ENDPOINT", "http://localhost:4318/v1/traces"),
    serviceName: String = "loki-engine"
  ): Task[Unit]
}
```

Sends an OTLP HTTP/JSON payload to any compatible backend (Tempo, Jaeger, Honeycomb, Datadog, Signoz, …). Span hierarchy: `hop → agent_start/end → tool_call/result`. No OTEL SDK dependency required.

---

## Browser Trace Visualizer

```scala
object TraceServer {
  // Blocking — serves until the process is killed
  def serve(ctx: ExecutionContext, port: Int = 8765, autoOpen: Boolean = true): Task[Nothing]

  // Non-blocking — returns the server fiber
  def serveBackground(ctx: ExecutionContext, port: Int = 8765): Task[Fiber.Runtime[Throwable, Nothing]]
}
```

Routes served:

| Path | Content |
|------|---------|
| `GET /` | Interactive HTML trace tree (color-coded, collapsible payloads) |
| `GET /data` | Full trace as JSON |
| `GET /explain` | `RunExplanation` as JSON |

---

## Symbolic Reasoning (Datalog Engine)

Located in `loki.engine.symbolic`. Import with `import loki.engine.symbolic.*`.

### Fact helpers

```scala
// Construct a ground fact
def fact(predicate: String, args: Any*): GroundFact   // List("parent", "alice", "bob")

// Encode a fact as a Map for JSON / agent consumption
def factToDict(f: GroundFact): Map[String, Any]        // {predicate: "parent", args: ["alice","bob"]}

// Normalise an arbitrary value into a GroundFact
def normaliseFact(item: Any): GroundFact
// Accepts: List[Any], Map("predicate"->..., "args"->...), or String atom

// Batch normalise
def atomsToFacts(items: Iterable[Any]): Set[GroundFact]

// Parse a Datalog program text into rules
def parseProgram(text: String): List[Rule]

// Convert a rule object (Rule | String | Map) to a Rule list
def ruleFromObj(obj: Any): List[Rule]
```

### Program

```scala
class Program(val rules: List[Rule]) {
  def solve(
    inputFacts: Iterable[Any] = Nil,
    withProof: Boolean = false
  ): Solution
}

object Program {
  def parse(text: String): Program
}
```

### Solution

```scala
case class Solution(
  facts: Set[GroundFact],                               // all facts (input + derived)
  derived: Set[GroundFact],                             // only newly inferred facts
  justifications: Map[GroundFact, (String, List[GroundFact])] = Map.empty
) {
  def query(predicate: String): List[GroundFact]        // filter by predicate name
  def explain(f: GroundFact): String                    // human-readable proof string
}
```

### Datalog syntax

Variables use uppercase-first or `?`-prefix notation. Constants are lowercase atoms, quoted strings, or numbers.

```datalog
// Transitivity
ancestor(X, Z) :- parent(X, Y), parent(Y, Z).
ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).

// Negation-as-failure (stratified)
safe(X) :- node(X), !infected(X).

// Comparison operators: = == != < <= > >=
big(X) :- size(X, N), N > 100.

// Wildcard (anonymous variable)
child(X) :- parent(_, X).

// Line comments with # or %
```

---

## Enums

### NodeType

```scala
enum NodeType(val value: String) {
  case Agent     extends NodeType("agent")
  case Tool      extends NodeType("tool")
  case Context   extends NodeType("context")
  case Prompt    extends NodeType("prompt")
  case Schema    extends NodeType("schema")
  case Graph     extends NodeType("graph")
  case Approval  extends NodeType("approval")
  case Transform extends NodeType("transform")
  case Reasoner  extends NodeType("reasoner")
}
```

### EdgeType

```scala
enum EdgeType(val value: String) {
  case HAS_TOOL     extends EdgeType("HAS_TOOL")
  case HAS_CONTEXT  extends EdgeType("HAS_CONTEXT")
  case HAS_PROMPT   extends EdgeType("HAS_PROMPT")
  case DELEGATES_TO extends EdgeType("DELEGATES_TO")
  case PRODUCES     extends EdgeType("PRODUCES")
  case MENTIONS     extends EdgeType("MENTIONS")
  case NEXT         extends EdgeType("NEXT")
  case COVERS       extends EdgeType("COVERS")
  case SIMILAR_TO   extends EdgeType("SIMILAR_TO")
  case VALIDATES    extends EdgeType("VALIDATES")
  case CONTAINS     extends EdgeType("CONTAINS")
}
```

### EventType

```scala
enum EventType(val value: String) {
  case AgentStart       extends EventType("agent_start")
  case AgentEnd         extends EventType("agent_end")
  case AgentSkip        extends EventType("agent_skip")
  case ToolCall         extends EventType("tool_call")
  case ToolResult       extends EventType("tool_result")
  case Routing          extends EventType("routing")
  case ContextInject    extends EventType("context_inject")
  case Hop              extends EventType("hop")
  case SubgraphEnter    extends EventType("subgraph_enter")
  case SubgraphExit     extends EventType("subgraph_exit")
  case Pause            extends EventType("pause")
  case Resume           extends EventType("resume")
  case Retry            extends EventType("retry")
  case Validation       extends EventType("validation")
  case PermissionDenied extends EventType("permission_denied")
  case Checkpoint       extends EventType("checkpoint")
  case Transaction      extends EventType("transaction")
  case ApprovalTask     extends EventType("approval_task")
  case Lease            extends EventType("lease")
  case Schedule         extends EventType("schedule")
  case Migration        extends EventType("migration")
  case Error            extends EventType("error")
}
```

### BatchStatus

```scala
enum BatchStatus(val value: String) {
  case Pending   extends BatchStatus("pending")
  case Running   extends BatchStatus("running")
  case Completed extends BatchStatus("completed")
  case Failed    extends BatchStatus("failed")
  case Partial   extends BatchStatus("partial")
}
```
