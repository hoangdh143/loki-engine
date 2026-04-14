package loki.engine.examples

import zio.*
import loki.engine.app.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.core.executor.*
import loki.engine.backend.*
import loki.engine.observability.*

object Example extends ZIOAppDefault {

  def run: ZIO[Any, Throwable, Unit] =
    for {
      store <- GraphStore.make
      (app, researcher, summarizer) <- buildGraph(store)

      // Run sequential
      _ = println("\n=== Sequential Execution ===")
      sequentialResult <- app.run(researcher.nodeId, "What is Scala ZIO?", strategy = "sequential")

      _ = println(s"Session ID: ${sequentialResult.sessionId}")
      _ = println(s"Hop count: ${sequentialResult.hopCount}")
      _ = println(s"Status: ${sequentialResult.state.status}")
      _ = println(s"Trace events: ${sequentialResult.trace.length}")

      // Print trace
      _ = printTrace(sequentialResult)
      _ = printSummary(explainRun(sequentialResult))

      // Run batch
      _ = println("\n=== Batch Execution ===")
      batchResult <- runBatch(store, app, summarizer)

      _ = println(s"Batch status: ${batchResult.status}")
      _ = println(s"Completed: ${batchResult.completed}/${batchResult.total}")
      _ = println(s"Progress: ${batchResult.progress * 100}%")
      _ <- ZIO.when(batchResult.reducedOutput != null) {
        ZIO.succeed(println(s"\nCombined output:\n${batchResult.reducedOutput}"))
      }

      // Cleanup
      cleaned <- cleanupSession(store, sequentialResult.sessionId)
      _ = println(s"\nCleaned up $cleaned runtime nodes")

      _ = println("\n=== Done ===")
    } yield ()

  /** The demo graph: a researcher delegating to a summarizer, with a search
    * tool and research-guideline context attached to the researcher. */
  private def buildGraph(store: GraphStore): ZIO[Any, Throwable, (GraphApp, AgentNode, AgentNode)] = {
    val researcher = createAgent(
      name = "Researcher",
      description = "Researches topics and finds information",
      model = "claude-sonnet-4-6",
      systemPrompt = "You are a helpful research assistant. Search for information and provide accurate summaries."
    )

    val summarizer = createAgent(
      name = "Summarizer",
      description = "Summarizes content",
      model = "claude-sonnet-4-6",
      systemPrompt = "You create concise summaries of provided content."
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

    val guidelines = createContext(
      name = "Research Guidelines",
      content = "Always cite sources. Verify facts before presenting. Be concise."
    )

    val app = new GraphApp(store)

    for {
      _ <- app.addAgent("Researcher", researcher)
      _ <- app.addAgent("Summarizer", summarizer)
      _ <- app.addTool("web_search", searchTool)
      _ <- app.addContext("Guidelines", guidelines)

      // Connect nodes
      _ <- app.connectTool(researcher.nodeId, searchTool.nodeId)
      _ <- app.connectContext(researcher.nodeId, guidelines.nodeId, weight = Some(0.9))
      _ <- app.delegate(researcher.nodeId, summarizer.nodeId)
    } yield (app, researcher, summarizer)
  }

  private def printSummary(explanation: RunExplanation): Unit = {
    println("\n=== Execution Summary ===")
    println(s"Routing decisions: ${explanation.summary.routingDecisionCount}")
    println(s"Tool calls: ${explanation.summary.toolCallCount}")
    println(s"Context injections: ${explanation.summary.contextInjectionCount}")
  }

  private def runBatch(store: GraphStore, app: GraphApp, summarizer: AgentNode): ZIO[Any, Throwable, BatchRun] =
    for {
      batchExec <- ZIO.succeed(new BatchExecutor(store, app.getExecutor, concurrency = 2))
      batch <- batchExec.run(
        agentNodeId = summarizer.nodeId,
        items = List(
          "Document 1 about machine learning",
          "Document 2 about functional programming",
          "Document 3 about distributed systems"
        ),
        queryFn = (doc: Any) => s"Summarize: $doc",
        reduceFn = Some((outputs: List[Any]) => outputs.mkString("\n---\n"))
      )
    } yield batch
}
