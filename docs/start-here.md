# Getting Started with Loki Engine

This guide walks you through building your first graph-native agent workflow.

## Installation

```bash
# Clone the repository
git clone https://github.com/your-org/loki-engine.git
cd loki-engine

# Build with sbt
sbt compile
```

## Your First Graph

Let's create a simple research agent that searches the web and summarizes results.

```scala
import zio.*
import loki.engine.app.*
import loki.engine.core.store.*
import loki.engine.core.nodes.*
import loki.engine.core.executor.*
import loki.engine.backend.*

val runtime = Runtime.default

Unsafe.unsafe { implicit u =>
  // 1. Create a graph store
  val store = runtime.unsafe.run(GraphStore.make).getOrThrow
  
  // 2. Create nodes
  val researcher = createAgent(
    name = "Researcher",
    model = "claude-sonnet-4-6",
    systemPrompt = "You are a research assistant. Search for information and provide a concise summary."
  )
  
  val searchTool = createTool(
    name = "web_search",
    callableRef = "loki.engine.tools.web_search.search",
    description = "Search the web for information",
    inputSchema = Map(
      "type" -> "object",
      "properties" -> Map(
        "query" -> Map("type" -> "string")
      ),
      "required" -> List("query")
    )
  )
  
  // 3. Create the app
  val app = new GraphApp(store)
  
  // 4. Add nodes to the graph
  runtime.unsafe.run(app.addAgent("Researcher", researcher)).getOrThrow
  runtime.unsafe.run(app.addTool("web_search", searchTool)).getOrThrow
  
  // 5. Connect the agent to the tool
  runtime.unsafe.run(app.connectTool(researcher.nodeId, searchTool.nodeId)).getOrThrow
  
  // 6. Run the graph
  val result = runtime.unsafe.run {
    app.run(researcher.nodeId, "What is Scala ZIO?")
  }.getOrThrow
  
  // 7. Inspect results
  println(s"Session: ${result.sessionId}")
  println(s"Trace: ${result.trace.length} events")
  println(s"Output: ${result.outputs.get(researcher.nodeId)}")
}
```

## Key Concepts

### Nodes

Nodes are the fundamental units in Loki Engine. Each node type has specific behavior:

- **AgentNode**: Executes an LLM with tools and context
- **ToolNode**: Executes a registered function
- **ContextNode**: Returns stored content
- **PromptNode**: Renders a template

### Edges

Edges connect nodes and define relationships:

```scala
// Agent → Tool relationship
Edge.hasTool(srcId = agent.nodeId, dstId = tool.nodeId, weight = 1.0)

// Agent → Context relationship (for context injection)
Edge.hasContext(srcId = agent.nodeId, dstId = ctx.nodeId, weight = 0.8)

// Agent delegation
Edge.delegatesTo(srcId = agent.nodeId, dstId = anotherAgent.nodeId)
```

### Execution Context

The `ExecutionContext` carries state through the graph:

```scala
case class ExecutionContext(
  query: Any,              // The input query
  sessionId: String,       // Unique run identifier
  outputs: Map[String, Any], // nodeId → output
  trace: List[TraceEvent], // Full execution history
  state: WorkflowState,    // Mutable workflow state
  maxHops: Int,            // Execution depth limit
  hopCount: Int            // Current execution depth
)
```

## Adding Context

Attach knowledge context to agents:

```scala
val guidelines = createContext(
  name = "Research Guidelines",
  content = "Always cite sources and verify facts before presenting them."
)

// Connect to agent with affinity weight
runtime.unsafe.run(app.connectContext(researcher.nodeId, guidelines.nodeId, weight = Some(0.9))).getOrThrow
```

## Using Routing

Create agents with multiple output paths:

```scala
val triage = createAgent(
  name = "TriageAgent",
  routingTable = Map(
    "billing" -> "billingAgent.nodeId",
    "technical" -> "techSupport.nodeId",
    "sales" -> "salesAgent.nodeId",
    "default" -> "__END__"
  )
)
```

## Next Steps

- Read [First Graph](./first-graph.md) for a complete walkthrough
- See [Architecture](./architecture.md) for system design
- Explore [Workflow Patterns](./workflow-patterns.md) for common patterns
