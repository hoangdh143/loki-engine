# Choose Your Backend

Loki Engine ships with two built-in LLM backends. Both use `java.net.http.HttpClient` (JDK 11+) — no extra HTTP library dependency is required.

## Anthropic (Recommended)

Best for production workloads requiring Claude models.

```scala
import loki.engine.backend.AnthropicBackend

// Reads ANTHROPIC_API_KEY from environment
val backend = new AnthropicBackend()

// Explicit API key
val backend = new AnthropicBackend(apiKey = "sk-ant-...")

// Custom endpoint (for proxies or private deployments)
val backend = new AnthropicBackend(
  baseUrl = "https://my-proxy.example.com/v1/messages",
  apiKey = "..."
)
```

## OpenAI-Compatible

Works with OpenAI, Ollama, mlx-lm, LM Studio, vLLM, Together AI, and any other OpenAI-compatible server.

```scala
import loki.engine.backend.OpenAIBackend

// Reads OPENAI_API_KEY + OPENAI_BASE_URL from environment
// Default baseUrl: https://api.openai.com/v1/chat/completions
val backend = new OpenAIBackend()

// Ollama (local)
val backend = new OpenAIBackend(
  baseUrl = "http://localhost:11434/v1/chat/completions",
  apiKey = ""  // Ollama doesn't require a key
)

// LM Studio
val backend = new OpenAIBackend(
  baseUrl = "http://localhost:1234/v1/chat/completions",
  apiKey = "lm-studio"
)

// vLLM / Together AI / Azure
val backend = new OpenAIBackend(
  baseUrl = "https://my-vllm-cluster.example.com/v1/chat/completions",
  apiKey = "..."
)
```

### LM Studio end-to-end tests

The Gherkin scenarios in `src/test/resources/features/lm_studio_app.feature`
exercise agents, attached context, tools, routing, and batch execution against
the real local model. Start LM Studio's server, load the model, and run the
standalone BDD suite:

```bash
lms server start
lms load prism-ml/bonsai-27b --identifier prism-ml/bonsai-27b -y
sbt "testOnly loki.engine.LMStudioBddTests"
```

The defaults are `http://localhost:1234/v1/chat/completions` and
`prism-ml/bonsai-27b`. Override the endpoint with `LM_STUDIO_BASE_URL` when
LM Studio is listening elsewhere.

## Custom Backend

Implement the `LLMBackend` trait for any LLM:

```scala
import zio.*
import loki.engine.backend.*

class MyLLMBackend extends LLMBackend {
  def chat(
    model: String,
    system: String,
    messages: List[Map[String, Any]],
    tools: List[Map[String, Any]],
    maxTokens: Int = 8096
  ): Task[LLMResponse] = ZIO.attemptBlocking {
    // Your HTTP call logic here
    LLMResponse(
      text = "response text",
      toolCalls = Nil,
      stopReason = "end_turn"
    )
  }

  def extendMessages(
    messages: List[Map[String, Any]],
    response: LLMResponse,
    toolResults: List[ToolResult]
  ): List[Map[String, Any]] = {
    // Append the assistant turn + tool results in your backend's native format
    messages ++
      List(Map[String, Any]("role" -> "assistant", "content" -> response.text)) ++
      toolResults.map(tr => Map[String, Any]("role" -> "tool", "content" -> tr.content))
  }
}
```

## Backend Comparison

| Backend | Pros | Cons |
|---------|------|------|
| Anthropic | Best models, reliable, native tool-use | Cost, API rate limits |
| OpenAI | Wide model choice, broad compatibility | Cost, rate limits |
| Ollama | Free, local, private | Slower, limited models |
| LM Studio | Free, GUI, easy local setup | Manual model management |
| vLLM | Fast, scalable, batching | Infrastructure required |

## Embedding

For semantic search and wRRF agent discovery, use the `Embedder` class:

```scala
import loki.engine.retrieval.Embedder

// Uses sentence-transformers via JVM reflection (requires Python + sentence-transformers installed)
val embedder = new Embedder(model = "all-MiniLM-L6-v2")

// Embed a single text
val vector: List[Double] = runtime.unsafe.run(embedder.embedText("my query")).getOrThrow

// Embed all nodes in the store (skips nodes that already have embeddings)
val count = runtime.unsafe.run(embedder.embedAll(store)).getOrThrow
```

To implement a custom embedder, implement the `loki.engine.backend.Embedder` trait:

```scala
import loki.engine.backend.Embedder

class MyEmbedder extends Embedder {
  def embedText(text: String): Task[List[Double]] = ZIO.attemptBlocking {
    // Call your embedding API and return a vector
    List.fill(1536)(0.0)  // example: text-embedding-3-small dimensions
  }
}
```

## Using Multiple Backends

```scala
// Main executor with production LLM
val productionBackend = new AnthropicBackend()
val executor = new GraphExecutor(store, productionBackend, composer)

// Cheaper/faster model for routing decisions
val executor = new GraphExecutor(
  store,
  backend = new AnthropicBackend(),
  composer = new AgentComposer(store),
  routerModel = "claude-haiku-4-5-20251001"  // Fast model for route/plan calls
)

// Or use Ollama for routing (free)
val executor = new GraphExecutor(
  store,
  backend = new AnthropicBackend(),   // Production agents use Anthropic
  composer = new AgentComposer(store),
  routerModel = "llama3"              // Override: routing calls hit Ollama
)
// Note: routerModel is the model name passed to backend.chat() for route() and plan() calls
```

## Environment Variables

```bash
# Anthropic
export ANTHROPIC_API_KEY="sk-ant-..."

# OpenAI / OpenAI-compatible
export OPENAI_API_KEY="sk-..."
export OPENAI_BASE_URL="https://api.openai.com/v1/chat/completions"

# OpenTelemetry export endpoint
export OTEL_ENDPOINT="http://localhost:4318/v1/traces"
```

## Best Practices

1. **Use Anthropic for production** — Best reliability and tool-use quality
2. **Use Ollama for development** — Fast iteration, no API cost
3. **Use `routerModel`** to choose a cheaper/faster model for `route()` and `plan()` calls
4. **Set `ExecutionPolicy` timeouts** to prevent hanging on API issues
5. **Use `RetryPolicy` with backoff** for transient rate-limit errors
