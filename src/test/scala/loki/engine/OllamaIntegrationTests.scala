package loki.engine

import zio.test.{test, assert, assertTrue, Assertion, Spec, suite, TestAspect}
import zio.test.Assertion.*
import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.core.executor.*
import loki.engine.backend.*
import loki.engine.observability.explainRun

/**
 * Integration tests that run against a local Ollama instance (llama3.1).
 *
 * Prerequisites:
 *   - Ollama running at http://localhost:11434
 *   - llama3.1 model pulled: `ollama pull llama3.1`
 *
 * Each test exercises a real end-to-end execution path through the engine
 * (store → executor → Ollama → tool/context resolution → trace).
 */
object OllamaIntegrationTests {

  private val ollamaBackend = new OpenAIBackend(
    baseUrl = "http://localhost:11434/v1/chat/completions",
    apiKey   = ""
  )
  private val model = "llama3.1"

  private def freshStore(): InMemoryGraphStore = new InMemoryGraphStore()

  private def makeExecutor(store: InMemoryGraphStore): GraphExecutor =
    new GraphExecutor(store, ollamaBackend, new AgentComposer(store))

  // ── 1. Basic single-agent execution ────────────────────────────────────────
  // Verifies: real LLM call succeeds, output is populated, trace is emitted.
  private val basicAgentTest = test("single agent produces a non-empty response via Ollama") {
    val store    = freshStore()
    val executor = makeExecutor(store)
    val agent = AgentNode(
      name         = "FactAgent",
      model        = model,
      systemPrompt = "You are a terse assistant. Respond with one word only."
    )
    for {
      _      <- store.upsertNode(agent)
      ctx    <- executor.run(agent.nodeId, "What is the capital of France?")
      output  = ctx.outputs.get(agent.nodeId).map(_.toString).getOrElse("")
      explain = explainRun(ctx)
    } yield {
      assertTrue(ctx.hopCount >= 1)        &&
      assertTrue(output.nonEmpty)          &&
      assertTrue(ctx.trace.nonEmpty)       &&
      assert(explain.hops.length)(isGreaterThanEqualTo(1))
    }
  }

  // ── 2. Context injection into agent execution ───────────────────────────────
  // Verifies: ContextNode connected via HAS_CONTEXT edge is injected into the
  // agent's system context; RunExplanation.contextInjectionCount >= 1.
  private val contextInjectionTest = test("agent with HAS_CONTEXT edge injects knowledge into execution") {
    val store    = freshStore()
    val executor = makeExecutor(store)
    val agent = AgentNode(
      name         = "ContextAgent",
      model        = model,
      systemPrompt = "Answer only from the context provided. Be concise."
    )
    val knowledge = ContextNode(
      name     = "ProjectFacts",
      content  = "Project NEBULA was launched on 15 March 2041 by Dr. Amara Nwosu.",
      priority = 10,
      tags     = List("project", "history")
    )
    val ctxEdge = Edge.hasContext(agent.nodeId, knowledge.nodeId, weight = 1.0)

    for {
      _       <- store.upsertNode(agent)
      _       <- store.upsertNode(knowledge)
      _       <- store.upsertEdge(ctxEdge)
      ctx     <- executor.run(agent.nodeId, "Who launched project NEBULA and when?")
      output   = ctx.outputs.get(agent.nodeId).map(_.toString).getOrElse("").toLowerCase
    } yield {
      assertTrue(ctx.hopCount >= 1)                              &&
      assertTrue(ctx.outputs.contains(agent.nodeId))             &&
      // Agent's response must reference facts that only exist in the injected context
      assertTrue(
        output.contains("nebula") ||
        output.contains("nwosu") ||
        output.contains("2041") ||
        output.contains("amara")
      )
    }
  }

  // ── 3. Tool-augmented agent (echo tool) ────────────────────────────────────
  // Verifies: ToolNode is registered, HAS_TOOL edge is present, and the agent
  // either invokes the tool (ToolCall trace event) or answers directly—both are
  // valid outcomes; the execution must complete without error either way.
  private val toolAugmentedAgentTest = test("agent with HAS_TOOL edge can invoke registered echo tool") {
    val store    = freshStore()
    val executor = makeExecutor(store)

    val tool = ToolNode(
      name        = "echo",
      description = "Echoes back the input text verbatim. Call it to repeat something.",
      callableRef = "loki.integration.echo",
      inputSchema = Map(
        "type"       -> "object",
        "properties" -> Map(
          "text" -> Map("type" -> "string", "description" -> "The text to echo back")
        ),
        "required" -> List("text")
      )
    )
    val agent = AgentNode(
      name          = "ToolAgent",
      model         = model,
      systemPrompt  = "You have access to an echo tool. Use it whenever asked to repeat text.",
      maxIterations = 4
    )
    // Register the callable
    executor.registerTool("loki.integration.echo", (input: Any) => {
      val m   = input.asInstanceOf[Map[String, Any]]
      val txt = m.getOrElse("text", "").toString
      s"ECHO: $txt"
    })
    val toolEdge = Edge.hasTool(agent.nodeId, tool.nodeId, weight = 1.0)

    for {
      _       <- store.upsertNode(agent)
      _       <- store.upsertNode(tool)
      _       <- store.upsertEdge(toolEdge)
      ctx     <- executor.run(agent.nodeId, "Please echo the phrase: hello world")
      output   = ctx.outputs.get(agent.nodeId).map(_.toString).getOrElse("")
      explain  = explainRun(ctx)
      // Accept: tool was called, OR the response text contains the phrase
      toolUsed = explain.summary.toolCallCount >= 1
      answered = output.toLowerCase.contains("hello") || output.toLowerCase.contains("echo")
    } yield {
      assertTrue(ctx.hopCount >= 1) &&
      assertTrue(output.nonEmpty)   &&
      assertTrue(toolUsed || answered)
    }
  }

  // ── 4. Multi-agent pipeline (deterministic routing) ────────────────────────
  // Orchestrator always routes to Specialist via its default routing table entry.
  // Verifies: both agents execute (hopCount >= 2), outputs keyed by both node IDs,
  // at least one routing decision is recorded in the trace.
  private val multiAgentPipelineTest = test("orchestrator routes every query to specialist and both agents execute") {
    val store    = freshStore()
    val executor = makeExecutor(store)

    val specialist = AgentNode(
      name         = "Specialist",
      model        = model,
      systemPrompt = "You are a specialist. Summarise the user's message in one sentence.",
      routingTable = Map("default" -> "__END__")
    )
    // Orchestrator's default route points directly to the specialist's nodeId.
    val orchestrator = AgentNode(
      name         = "Orchestrator",
      model        = model,
      systemPrompt = "You are an orchestrator. Acknowledge the task, then forward it.",
      routingTable = Map("default" -> specialist.nodeId)
    )

    for {
      _       <- store.upsertNode(orchestrator)
      _       <- store.upsertNode(specialist)
      ctx     <- executor.run(orchestrator.nodeId, "Describe the water cycle in 3 steps.")
      explain  = explainRun(ctx)
    } yield {
      assert(ctx.hopCount)(isGreaterThanEqualTo(2))                        &&
      assertTrue(ctx.outputs.contains(orchestrator.nodeId))                &&
      assertTrue(ctx.outputs.contains(specialist.nodeId))                  &&
      assert(explain.summary.routingDecisionCount)(isGreaterThanEqualTo(1))
    }
  }

  // ── 5. Concurrent batch execution with reduce ──────────────────────────────
  // Sends 3 documents through a single agent in parallel (concurrency = 3).
  // Verifies: all items complete, no failures, and the reduce function merges
  // outputs into a single string.
  private val batchExecutionTest = test("batch executor runs 3 docs concurrently and reduces outputs") {
    val store    = freshStore()
    val executor = makeExecutor(store)

    val agent = AgentNode(
      name         = "BatchAgent",
      model        = model,
      systemPrompt = "Reply with a single sentence takeaway from the text given.",
      routingTable = Map("default" -> "__END__")
    )
    val docs = List(
      "Photosynthesis converts sunlight into glucose in plant cells.",
      "The Eiffel Tower was completed in 1889 and stands 330 metres tall.",
      "Quantum entanglement correlates particles instantaneously across any distance."
    )

    for {
      _          <- store.upsertNode(agent)
      batchExec   = new BatchExecutor(store, executor, concurrency = 3)
      run        <- batchExec.run(
                      agentNodeId = agent.nodeId,
                      items       = docs,
                      queryFn     = (doc: Any) => s"Summarise: $doc",
                      reduceFn    = Some((outputs: List[Any]) => outputs.mkString("\n---\n"))
                    )
    } yield {
      assert(run.total)(equalTo(3))     &&
      assert(run.completed)(equalTo(3)) &&
      assert(run.failed)(equalTo(0))    &&
      assertTrue(run.reducedOutput != null && run.reducedOutput.toString.nonEmpty)
    }
  }

  // ── suite ──────────────────────────────────────────────────────────────────
  def spec =
    suite("OllamaIntegrationTests")(
      basicAgentTest,
      contextInjectionTest,
      toolAugmentedAgentTest,
      multiAgentPipelineTest,
      batchExecutionTest
    ) @@ TestAspect.timeout(3.minutes) @@ TestAspect.sequential
}
