# Advanced Configuration

Customizing Loki Engine for production use.

## Custom LLM Backend

Implement the `LLMBackend` trait to use any LLM provider. For Ollama specifically, use the built-in `OpenAIBackend` — it is already OpenAI-compatible:

```scala
import loki.engine.backend.OpenAIBackend

// Ollama (no API key required)
val backend = new OpenAIBackend(
  baseUrl = "http://localhost:11434/v1/chat/completions",
  apiKey = ""
)
```

For a fully custom backend, implement `LLMBackend` using `java.net.http.HttpClient`:

```scala
import zio.*
import loki.engine.backend.*
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI

class MyLLMBackend extends LLMBackend {
  private val client = HttpClient.newHttpClient()

  def chat(
    model: String,
    system: String,
    messages: List[Map[String, Any]],
    tools: List[Map[String, Any]],
    maxTokens: Int = 8096
  ): Task[LLMResponse] = ZIO.attemptBlocking {
    // Build your request body, send via client, parse response
    LLMResponse(
      text = "...",
      toolCalls = Nil,
      stopReason = "end_turn"
    )
  }

  def extendMessages(
    messages: List[Map[String, Any]],
    response: LLMResponse,
    toolResults: List[ToolResult]
  ): List[Map[String, Any]] = {
    messages ++
      List(Map[String, Any]("role" -> "assistant", "content" -> response.text)) ++
      toolResults.map(tr => Map[String, Any]("role" -> "tool", "content" -> tr.content))
  }
}

// Usage
val backend = new MyLLMBackend()
val executor = new GraphExecutor(store, backend, composer)
```

## Reasoning-token and history limits

Set the initial output budget on each agent. When a provider reports that an
answer was truncated at the token limit before producing text or a tool call,
the executor retries with a larger, bounded budget.

```scala
val agent = AgentNode(
  name = "researcher",
  maxTokens = 12000
)
```

Long tool-using loops also compact older message bodies while preserving the
initial task, recent turns, and tool-call/result pairing. Both behaviors can be
tuned with environment variables:

```bash
export LOKI_MAX_TOKEN_ESCALATIONS=2
export LOKI_TOKEN_ESCALATION_FACTOR=2
export LOKI_TOKEN_ESCALATION_CAP=32000

export LOKI_HISTORY_COMPACT_EVERY=6
export LOKI_HISTORY_HEAD_KEEP=2
export LOKI_HISTORY_RECENT_KEEP=6
export LOKI_HISTORY_ELIDE_CHARS=600
```

Set `LOKI_MAX_TOKEN_ESCALATIONS=0` to disable token-budget retries, or
`LOKI_HISTORY_COMPACT_EVERY=0` to disable rolling history compaction.

The three escalation knobs are read **when they are used**, not when the engine
class first loads, so a host that picks its ceiling from the model it just
loaded can still be heard. The JVM's environment is fixed for the life of the
process, so the live channel is a system property — each variable is also
readable in lower-case dotted form, and the property wins:

```scala
// Honoured by the very next agent turn.
System.setProperty("loki.token.escalation.cap", "13200")
```

A 22000-token-context model asked for the 32000-token default can never return:
the turn truncates, escalates, and truncates again. Nothing fails loudly — the
run just crawls — so being able to lower the cap at runtime matters.

## Tolerating what a local model actually does

A local model served through an OpenAI-compatible endpoint fails in ways a
hosted frontier model does not, and each one used to end an agent step
mid-task. All of these recoveries are bounded, and setting a knob to `0`
restores the previous behaviour.

```bash
export LOKI_MAX_TOOL_CALL_CORRECTIONS=3   # tool call the server left as text
export LOKI_MAX_CONTINUATION_NUDGES=2     # blank or announce-and-stop final turn
export LOKI_MAX_COMPLETION_NUDGES=2       # declared completion_paths still missing
export LOKI_SKIP_COMPLETED_AGENTS=1       # 0 to always run the agent
```

**Tool calls written as text.** When the server's parser does not recognise a
model's dialect it leaves the markup in `content` — or, for a reasoning model,
in `reasoning_content` with `content` blank. The backend reads both channels and
recovers Hermes/Qwen JSON, Llama-3 `<function=…><parameter=…>`, and the hybrid
of the two into real tool calls. Only unparseable markup falls through as
`stopReason == "unparsed_tool_call"`, which the executor answers with a bounded
re-prompt. Recovering beats re-prompting: no turn is wasted, and a small model
often cannot comply with the correction anyway.

Arguments the *server* mangled are not executed. A half-parsed XML call can
arrive as a real tool call whose arguments are markup fragments, every real
argument lost; the tool would then silently answer a different question, and a
wrong answer is worse than a failed call.

**Argument types.** A comma-separated string for an array parameter
(`{"tags": "new,sale"}`) and a stringified number or boolean are coerced to
what the tool's `inputSchema` declares, before validation. Only
schema-declared, lossless conversions — an undeclared key and an unparseable
value are passed through untouched for validation to judge.

**Turns that are not answers.** A blank final turn, or one that reads as an
announcement of the next action ("Now let me resolve all the field contexts:"),
is nudged rather than accepted as the agent's result. A blank turn whose
reasoning channel reached a real conclusion is salvaged as before, not nudged.

**Unknown tools name the belt.** `Tool 'read_file' not found.` is a dead end —
the model cannot learn the belt from it, so it retries the same absent tool or
gives up. The error now lists the tools that do exist (and tells a node with no
tools to answer directly). There is deliberately no "did you mean X?" guess: a
lexical match is blind to intent and scores `read_file` closer to `write_file`
than to `read_input`.

## Artefacts that define "done"

An agent whose work *is* the files it leaves behind has not finished while they
are absent, however confident its closing message sounds. Declare them on the
node and the engine checks both directions:

```scala
val writer = AgentNode(
  name = "report-writer",
  attributes = Map("completion_paths" -> List("out/report.md", "out/summary.json"))
)
```

- A terminal turn with any declared path **missing** is not done: the agent is
  told which files are absent and asked to finish (bounded by
  `LOKI_MAX_COMPLETION_NUDGES`).
- When **every** declared path already exists before the first turn, the agent
  is skipped entirely and an `agent_skip` trace event is emitted. On resume a
  fan-out otherwise re-executes every agent node, and each "already done"
  discovery costs a full LLM turn. Prompt-level idempotency guards cannot close
  this — a lean tool belt has no file-existence tool, so the instruction is
  unsatisfiable. The engine is the only place that can check.

The contract is existence only; loki never interprets the contents. A node that
declares no `completion_paths` is untouched by either check.

## Custom Embedder

Implement `loki.engine.backend.Embedder` to use any embedding API:

```scala
import zio.*
import loki.engine.backend.Embedder
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI

class OpenAIEmbedder(apiKey: String) extends Embedder {
  private val client = HttpClient.newHttpClient()

  def embedText(text: String): Task[List[Double]] = ZIO.attemptBlocking {
    val body = s"""{"input":"${text.replace("\"", "\\\"")}","model":"text-embedding-3-small"}"""
    val req = HttpRequest.newBuilder(URI.create("https://api.openai.com/v1/embeddings"))
      .header("Authorization", s"Bearer $apiKey")
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    // Parse embedding from resp.body() — returns List[Double]
    List.fill(1536)(0.0)  // placeholder; replace with actual JSON parsing
  }
}

val embedder = new OpenAIEmbedder(sys.env("OPENAI_API_KEY"))
```

## Context Navigator Tuning

```scala
val navigator = new ContextNavigator(
  maxHops = 3,              // Expand up to 3 hops
  maxContextNodes = 10,     // Include up to 10 context nodes
  maxContextTokens = 8000,  // Budget 8000 tokens for context
  semanticTopK = 20,        // Retrieve top 20 semantic matches
  perSourceLimit = 3,       // Max 3 from same source
  pathDecay = 0.8,          // Decay factor per hop
  tagWeight = 0.2,          // Weight for tag overlap
  priorityWeight = 0.1,     // Weight for priority field
  recencyWeight = 0.15,     // Weight for recency
  provenanceWeight = 0.2,   // Weight for runtime provenance
  staleFactPenalty = 0.5,   // Penalty for stale facts
  excludeStaleFacts = true  // Exclude invalid facts
)

val composer = new AgentComposer(store, Some(embedder), navigator)
val executor = new GraphExecutor(store, backend, composer)
```

## Execution Policies

### Per-Tool Retry

```scala
val reliableTool = createTool(
  name = "api_call",
  callableRef = "mymodule.api",
  executionPolicy = ExecutionPolicy(
    timeoutSeconds = Some(30.0),
    retryPolicy = RetryPolicy(
      maxAttempts = 5,
      backoffSeconds = 2.0,
      backoffMultiplier = 2.0
    ),
    idempotencyKey = "request_id"
  )
)
```

### Per-Agent Execution

```scala
val agent = createAgent(
  name = "CarefulAgent",
  executionPolicy = ExecutionPolicy(
    timeoutSeconds = Some(120.0),
    retryPolicy = RetryPolicy(
      maxAttempts = 2,
      backoffSeconds = 5.0,
      backoffMultiplier = 1.5
    )
  )
)
```

## Multi-Tenancy

Use `groupId` to partition graphs:

```scala
val tenant1Agent = createAgent(
  name = "Tenant1Agent",
  groupId = "tenant-1"
)

val tenant2Agent = createAgent(
  name = "Tenant2Agent", 
  groupId = "tenant-2"
)

// Query by tenant
val tenant1Nodes = runtime.unsafe.run {
  store.listNodes(groupId = Some("tenant-1"))
}
```

## Temporal Validity

Nodes and edges can expire:

```scala
// Node valid from now until 30 days
val temporaryCtx = createContext(
  name = "Holiday Sale Info",
  content = "20% off until Jan 15",
  validAt = Instant.now(),
  invalidAt = Some(Instant.now().plusSeconds(30 * 24 * 60 * 60))
)

// Soft expire a node
runtime.unsafe.run(store.expireNode(node.nodeId))

// Query only valid nodes
val validNodes = runtime.unsafe.run {
  store.listNodes(onlyValid = true)
}
```

## Event Hooks

```scala
// Logging hook
executor.addEventHook((event, ctx) => {
  event.eventType match {
    case EventType.AgentEnd =>
      logger.info(s"Agent ${event.nodeName} completed in ${event.durationMs}ms")
    case EventType.Error =>
      logger.error(s"Error in ${event.nodeName}: ${event.payload.get("error")}")
    case _ =>
  }
})

// Metrics hook
executor.addEventHook((event, ctx) => {
  event.eventType match {
    case EventType.ToolCall =>
      metrics.increment("tool_calls", Map("tool" -> event.nodeName))
    case EventType.Routing =>
      metrics.gauge("routing_confidence", 
        event.payload.get("confidence").map(_.asInstanceOf[Double]).getOrElse(1.0))
    case _ =>
  }
})
```

## Serializing State for Checkpointing

`ctx.snapshot` returns a `Map[String, Any]` that you can serialize with any JSON library and persist externally. To resume a paused workflow, restore the `ExecutionContext` manually from the snapshot fields:

```scala
// Serialize with any JSON library (e.g. uPickle, play-json, jackson)
val snapshot: Map[String, Any] = ctx.snapshot
// persist snapshot to database, file, etc.

// To resume: reconstruct ExecutionContext from snapshot fields
val restoredCtx = ctx.copy(
  state = ctx.state.copy(
    status = "running",
    pendingPause = None
  )
)

val resumed = runtime.unsafe.run {
  executor.resume(entryNodeId, restoredCtx)
}.getOrThrow
```

## Graph Revision History

```scala
// Begin a new revision
val newRevision = runtime.unsafe.run {
  store.upsertNode(agent.copy(version = 2))
}

// Query historical state
val history = runtime.unsafe.run {
  store.listNodes(groupId = Some("default"), onlyValid = false)
}
```

## Performance Tuning

### Batch Size

```scala
// For large batches, increase concurrency
val batchExec = new BatchExecutor(store, executor, concurrency = 20)

// Or use checkpointing to resume if interrupted
val result = runtime.unsafe.run {
  batchExec.run(agent.nodeId, items, queryFn, checkpoint = true)
}
```

### Vector Search

```scala
// For large graphs, use approximate nearest neighbor
class HNSWGraphStore(/* config */) extends GraphStore {
  private val hnsw = new HNSWIndex(dimension = 384)
  
  def vectorSearch(embedding: List[Double], ...): UIO[List[(AnyNode, Double)]] = 
    ZIO.succeed(hnsw.search(embedding, topK))
}
```
