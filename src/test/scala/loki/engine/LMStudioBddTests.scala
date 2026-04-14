package loki.engine

import zio.*
import zio.test.{assertTrue, suite, test, TestAspect, ZIOSpecDefault}
import loki.engine.backend.*
import loki.engine.core.edges.Edge
import loki.engine.core.executor.*
import loki.engine.core.nodes.*
import loki.engine.core.store.InMemoryGraphStore

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.io.Source
import scala.util.matching.Regex

object LMStudioBddTests extends ZIOSpecDefault {
  private val FeatureResource = "features/lm_studio_app.feature"
  private val BaseUrl = sys.env.getOrElse("LM_STUDIO_BASE_URL", "http://localhost:1234/v1/chat/completions")

  private case class Scenario(name: String, steps: List[String])
  private case class Feature(name: String, scenarios: List[Scenario])

  private case class World(
    model: String = "",
    store: Option[InMemoryGraphStore] = None,
    executor: Option[GraphExecutor] = None,
    currentAgent: Option[AgentNode] = None,
    orchestrator: Option[AgentNode] = None,
    specialist: Option[AgentNode] = None,
    context: Option[ExecutionContext] = None,
    batch: Option[BatchRun] = None
  )

  private val Quoted: Regex = """.*\"([^\"]+)\".*""".r

  private val feature = parseFeature(FeatureResource)

  override def spec =
    suite(feature.name)(
      feature.scenarios.map { scenario =>
        test(scenario.name) {
          ZIO.foldLeft(scenario.steps)(World())((world, step) => runStep(step, world))
            .as(assertTrue(true))
        }
      }*
    ) @@ TestAspect.sequential @@ TestAspect.timeout(12.minutes)

  private def runStep(step: String, world: World): Task[World] = step match {
    case s if s.startsWith("LM Studio is serving")                 => lmStudioReadyStep(s, world)
    case s if s.startsWith("an agent that must answer with")       => exactAnswerStep(s, world)
    case "an agent that may answer only from attached context"     => groundedAgentStep(world)
    case s if s.startsWith("context containing") && s.endsWith("is attached") => attachContextStep(s, world)
    case "an agent required to use an echo tool"                   => echoToolStep(world)
    case "an orchestrator that always routes to a specialist"      => orchestratorStep(world)
    case "a batch summarizer agent"                                => batchAgentStep(world)
    case s if s.startsWith("I run the agent with")                 => runCurrentAgent(world, quotedValue(s))
    case s if s.startsWith("I ask the agent to echo")              => runCurrentAgent(world, s"Use echo_tool to echo exactly: ${quotedValue(s)}")
    case s if s.startsWith("I run the orchestrator with")          => runCurrentAgent(world, quotedValue(s))
    case "I process 2 documents concurrently"                      => batchRunStep(world)
    case s if isAssertionStep(s)                                   => assertStep(s, world)
    case unknown => ZIO.fail(new IllegalArgumentException(s"Undefined Gherkin step: $unknown"))
  }

  private def lmStudioReadyStep(step: String, world: World): Task[World] = {
    val model = quotedValue(step)
    for {
      _ <- verifyLMStudio(model)
      store = new InMemoryGraphStore()
      backend = new OpenAIBackend(baseUrl = BaseUrl, apiKey = "lm-studio", temperature = Some(0.0))
      executor = new GraphExecutor(store, backend, new AgentComposer(store))
    } yield world.copy(model = model, store = Some(store), executor = Some(executor))
  }

  private def exactAnswerStep(step: String, world: World): Task[World] = {
    val expected = quotedValue(step)
    addAgent(
      world,
      AgentNode(
        name = "ExactAnswerAgent",
        model = world.model,
        systemPrompt = s"Answer with exactly $expected and nothing else.",
        maxTokens = 1024
      )
    )
  }

  private def groundedAgentStep(world: World): Task[World] =
    addAgent(
      world,
      AgentNode(
        name = "GroundedAgent",
        model = world.model,
        systemPrompt = "Answer only from attached context. Return only the requested value.",
        maxTokens = 1024
      )
    )

  private def attachContextStep(step: String, world: World): Task[World] = {
    val content = quotedValue(step)
    val store = required(world.store, "store")
    val agent = required(world.currentAgent, "current agent")
    val context = ContextNode(name = "PrivateFacts", content = content, priority = 10)
    store.upsertNode(context) *>
      store.upsertEdge(Edge.hasContext(agent.nodeId, context.nodeId, weight = 1.0))
        .as(world)
  }

  private def echoToolStep(world: World): Task[World] = {
    val store = required(world.store, "store")
    val executor = required(world.executor, "executor")
    val tool = ToolNode(
      name = "echo_tool",
      description = "Echo the supplied text. You must use this tool whenever the user asks you to echo text.",
      callableRef = "bdd.echo",
      inputSchema = Map(
        "type" -> "object",
        "properties" -> Map("text" -> Map("type" -> "string")),
        "required" -> List("text")
      )
    )
    val agent = AgentNode(
      name = "ToolAgent",
      model = world.model,
      systemPrompt = "Always call echo_tool exactly once when asked to echo text. After the tool result, return it verbatim.",
      maxIterations = 5,
      maxTokens = 1024
    )
    executor.registerTool("bdd.echo", (input: Any) => {
      val text = input.asInstanceOf[Map[String, Any]].getOrElse("text", "").toString
      s"ECHO_RESULT: $text"
    })
    store.upsertNode(tool) *>
      store.upsertNode(agent) *>
      store.upsertEdge(Edge.hasTool(agent.nodeId, tool.nodeId, weight = 1.0))
        .as(world.copy(currentAgent = Some(agent)))
  }

  private def orchestratorStep(world: World): Task[World] = {
    val store = required(world.store, "store")
    val specialist = AgentNode(
      name = "Specialist",
      model = world.model,
      systemPrompt = "Summarize the request in one short sentence.",
      routingTable = Map("default" -> "__END__"),
      maxTokens = 1024
    )
    val orchestrator = AgentNode(
      name = "Orchestrator",
      model = world.model,
      systemPrompt = "Acknowledge the request briefly so it can be delegated.",
      routingTable = Map("default" -> specialist.nodeId),
      maxTokens = 1024
    )
    store.upsertNode(specialist) *>
      store.upsertNode(orchestrator)
        .as(world.copy(
          currentAgent = Some(orchestrator),
          orchestrator = Some(orchestrator),
          specialist = Some(specialist)
        ))
  }

  private def batchAgentStep(world: World): Task[World] =
    addAgent(
      world,
      AgentNode(
        name = "BatchSummarizer",
        model = world.model,
        systemPrompt = "Return one short factual takeaway from the supplied document.",
        maxTokens = 1024
      )
    )

  private def batchRunStep(world: World): Task[World] = {
    val store = required(world.store, "store")
    val executor = required(world.executor, "executor")
    val agent = required(world.currentAgent, "current agent")
    val documents = List(
      "Honeybees communicate flower locations through a waggle dance.",
      "Saturn is the sixth planet from the Sun and has a prominent ring system."
    )
    new BatchExecutor(store, executor, concurrency = 2).run(
      agentNodeId = agent.nodeId,
      items = documents,
      queryFn = (document: Any) => s"Summarize: $document",
      reduceFn = Some((outputs: List[Any]) => outputs.mkString("\n---\n"))
    ).map(run => world.copy(batch = Some(run)))
  }

  private val AssertionSteps = List(
    "the final output contains",
    "agent start and end events are traced",
    "a context injection event is traced",
    "at least one tool call is traced",
    "outputs are recorded for both agents",
    "a routing decision is traced",
    "every batch item completes successfully",
    "the reduced batch output is not empty"
  )

  private def isAssertionStep(step: String): Boolean =
    AssertionSteps.exists(prefix => step == prefix || step.startsWith(prefix))

  private def assertStep(step: String, world: World): Task[World] = {
    val trace = world.context.map(_.trace)
    step match {
      case s if s.startsWith("the final output contains") =>
        val expected = quotedValue(s)
        ensure(outputText(world).toUpperCase.contains(expected.toUpperCase),
          s"Expected final output to contain '$expected', got: ${outputText(world)}").as(world)

      case "agent start and end events are traced" =>
        requireEvent(trace, EventType.AgentStart, "Missing agent_start trace event") *>
          requireEvent(trace, EventType.AgentEnd, "Missing agent_end trace event").as(world)

      case "a context injection event is traced" =>
        requireEvent(trace, EventType.ContextInject, "Missing context_inject trace event").as(world)

      case "at least one tool call is traced" =>
        requireEvent(trace, EventType.ToolCall, "The model did not invoke echo_tool").as(world)

      case "outputs are recorded for both agents" =>
        val ctx = required(world.context, "execution context")
        val orchestrator = required(world.orchestrator, "orchestrator")
        val specialist = required(world.specialist, "specialist")
        ensure(ctx.outputs.contains(orchestrator.nodeId), "Missing orchestrator output") *>
          ensure(ctx.outputs.contains(specialist.nodeId), "Missing specialist output")
            .as(world)

      case "a routing decision is traced" =>
        requireEvent(trace, EventType.Routing, "Missing routing trace event").as(world)

      case "every batch item completes successfully" =>
        val batch = required(world.batch, "batch result")
        ensure(batch.completed == batch.total && batch.failed == 0,
          s"Expected ${batch.total} completed and 0 failed; got ${batch.completed} completed and ${batch.failed} failed").as(world)

      case "the reduced batch output is not empty" =>
        val batch = required(world.batch, "batch result")
        ensure(batch.reducedOutput != null && batch.reducedOutput.toString.trim.nonEmpty,
          "Reduced batch output was empty").as(world)

      case unknown => ZIO.fail(new IllegalArgumentException(s"Undefined Gherkin step: $unknown"))
    }
  }

  private def requireEvent(trace: Option[List[TraceEvent]], eventType: EventType, message: String): Task[Unit] =
    ensure(trace.exists(_.exists(_.eventType == eventType)), message)

  private def addAgent(world: World, agent: AgentNode): Task[World] =
    required(world.store, "store").upsertNode(agent).as(world.copy(currentAgent = Some(agent)))

  private def runCurrentAgent(world: World, query: String): Task[World] =
    required(world.executor, "executor")
      .run(required(world.currentAgent, "current agent").nodeId, query)
      .map(ctx => world.copy(context = Some(ctx)))

  private def outputText(world: World): String = {
    val ctx = required(world.context, "execution context")
    val agent = required(world.currentAgent, "current agent")
    ctx.outputs.get(agent.nodeId) match {
      case Some(output: Map[?, ?]) =>
        output.asInstanceOf[Map[String, Any]].get("text").map(_.toString).getOrElse(output.toString)
      case Some(output) => output.toString
      case None         => ""
    }
  }

  private def verifyLMStudio(model: String): Task[Unit] = ZIO.attemptBlocking {
    val modelsUrl = BaseUrl.stripSuffix("/chat/completions") + "/models"
    val request = HttpRequest.newBuilder(URI.create(modelsUrl)).GET().build()
    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200)
      throw new IllegalStateException(s"LM Studio returned HTTP ${response.statusCode()} from $modelsUrl")
    if (!response.body().contains(s"\"id\": \"$model\"") && !response.body().contains(s"\"id\":\"$model\""))
      throw new IllegalStateException(s"LM Studio is not serving model '$model': ${response.body().take(500)}")
  }

  private def ensure(condition: => Boolean, message: => String): Task[Unit] =
    ZIO.attempt(if (!condition) throw new AssertionError(message))

  private def required[A](value: Option[A], name: String): A =
    value.getOrElse(throw new IllegalStateException(s"Gherkin world has no $name"))

  private def quotedValue(step: String): String = step match {
    case Quoted(value) => value
    case _             => throw new IllegalArgumentException(s"Expected a quoted value in step: $step")
  }

  private def parseFeature(resource: String): Feature = {
    val source = Source.fromResource(resource)
    try {
      val lines = source.getLines().map(_.trim).filter(line => line.nonEmpty && !line.startsWith("#")).toList
      val featureName = lines.collectFirst { case line if line.startsWith("Feature:") => line.stripPrefix("Feature:").trim }
        .getOrElse(throw new IllegalArgumentException(s"No Feature declaration in $resource"))
      val backgroundStart = lines.indexWhere(_.startsWith("Background:"))
      val firstScenario = lines.indexWhere(_.startsWith("Scenario:"))
      val backgroundSteps = if (backgroundStart >= 0 && firstScenario > backgroundStart)
        lines.slice(backgroundStart + 1, firstScenario).flatMap(stepText)
      else Nil
      val scenarios = lines.zipWithIndex.collect { case (line, index) if line.startsWith("Scenario:") =>
        val end = lines.indexWhere(_.startsWith("Scenario:"), index + 1) match {
          case -1   => lines.length
          case next => next
        }
        Scenario(line.stripPrefix("Scenario:").trim, backgroundSteps ++ lines.slice(index + 1, end).flatMap(stepText))
      }
      if (scenarios.isEmpty) throw new IllegalArgumentException(s"No scenarios in $resource")
      Feature(featureName, scenarios)
    } finally source.close()
  }

  private def stepText(line: String): Option[String] =
    List("Given ", "When ", "Then ", "And ", "But ").collectFirst {
      case keyword if line.startsWith(keyword) => line.stripPrefix(keyword).trim
    }
}
