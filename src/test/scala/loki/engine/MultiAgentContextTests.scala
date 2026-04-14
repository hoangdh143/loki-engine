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
 * Integration tests for multi-agent workflows where agents consume context
 * produced by other agents.
 *
 * Three patterns exercised:
 *   A. Runtime ContextNode bridge — agent A runs, its text output is upserted
 *      as a ContextNode (origin=runtime) and linked via HAS_CONTEXT to agent B.
 *      The ContextNavigator picks it up automatically during B's composition.
 *   B. Routing chain with _workflow_state — A routes to B to C via routingTable;
 *      prior agent outputs are threaded through state.data["_workflow_state"]
 *      so each successive agent sees them as an injected assistant message.
 *   C. Parallel researchers + synthesizer — two agents run independently,
 *      each output is bridged as a runtime ContextNode; a third agent
 *      synthesises both via two HAS_CONTEXT edges.
 *
 * Prerequisites: Ollama running at http://localhost:11434 with llama3.1 pulled.
 */
object MultiAgentContextTests {

  private val backend = new OpenAIBackend(
    baseUrl = "http://localhost:11434/v1/chat/completions",
    apiKey   = ""
  )
  private val model = "llama3.1"

  // ── helpers ────────────────────────────────────────────────────────────────

  private def freshEnv(): (InMemoryGraphStore, GraphExecutor) = {
    val store    = new InMemoryGraphStore()
    val executor = new GraphExecutor(store, backend, new AgentComposer(store))
    (store, executor)
  }

  /** Extract the plain text from an agent node's execution output. */
  private def agentText(ctx: ExecutionContext, nodeId: String): String =
    ctx.outputs.get(nodeId).map {
      case m: Map[_, _] => m.asInstanceOf[Map[String, Any]].get("text").map(_.toString).getOrElse("")
      case s             => s.toString
    }.getOrElse("")

  /** Create a runtime ContextNode from a string and attach it to an agent. */
  private def bridgeAsContext(
    store:    InMemoryGraphStore,
    agentId:  String,
    content:  String,
    name:     String,
    sessionId: String
  ): UIO[ContextNode] =
    for {
      runtimeCtx <- ZIO.succeed(
        ContextNode(
          name       = name,
          content    = content,
          source     = "agent-output",
          priority   = 10,
          groupId    = sessionId,
          attributes = Map("origin" -> "runtime")
        )
      )
      _          <- store.upsertNode(runtimeCtx)
      _          <- store.upsertEdge(Edge.hasContext(agentId, runtimeCtx.nodeId, weight = 1.0))
    } yield runtimeCtx

  // ── Test 1: Extractor → Analyst (runtime ContextNode bridge) ───────────────
  // Pattern A:
  //   Phase 1 — Extractor reads a short text and lists key facts.
  //   Phase 2 — Analyst has the extracted facts injected as a runtime context
  //             and answers a question that can only be answered using those facts.
  private val extractorAnalystTest =
    test("extractor output bridged as runtime context is used by downstream analyst") {
      val (store, executor) = freshEnv()

      val extractor = AgentNode(
        name         = "Extractor",
        model        = model,
        systemPrompt =
          "You are a fact extractor. Given a text, list the key facts as bullet points. Be concise."
      )
      val analyst = AgentNode(
        name         = "Analyst",
        model        = model,
        systemPrompt =
          "You are an analyst. Use ONLY the context provided under '## Relevant Context' to answer the question. " +
          "Do not use outside knowledge."
      )

      val sourceDocument =
        "The Aurora-7 mission launched on 12 June 2044. " +
        "Its crew of three included Commander Yuki Tanaka, Dr. Priya Mehta, and Engineer Lars Nilsson. " +
        "The mission objective was to deploy the Helios solar array in geostationary orbit."

      for {
        // ── Phase 1: run extractor ────────────────────────────────────────
        _         <- store.upsertNode(extractor)
        extractCtx <- executor.run(extractor.nodeId, s"Extract facts from: $sourceDocument")
        facts      = agentText(extractCtx, extractor.nodeId)

        // ── Bridge: materialise extractor output as runtime context ────────
        _         <- store.upsertNode(analyst)
        _         <- bridgeAsContext(store, analyst.nodeId, facts, "ExtractedFacts", extractCtx.sessionId)

        // ── Phase 2: run analyst with the same session ──────────────────
        analystCtx <- executor.run(
                        analyst.nodeId,
                        "Who was the commander of Aurora-7 and what was its mission objective?",
                        executionContext = Some(extractCtx.copy(hopCount = 0, outputs = Map.empty))
                      )
        answer = agentText(analystCtx, analyst.nodeId).toLowerCase
      } yield {
        assertTrue(facts.nonEmpty)                                    &&
        assertTrue(analystCtx.hopCount >= 1)                          &&
        assertTrue(analystCtx.outputs.contains(analyst.nodeId))       &&
        // Analyst must reference facts only present in the injected context
        assertTrue(
          answer.contains("tanaka") ||
          answer.contains("yuki")   ||
          answer.contains("solar")  ||
          answer.contains("helios") ||
          answer.contains("aurora")
        )
      }
    }

  // ── Test 2: Three-stage writing pipeline via _workflow_state ───────────────
  // Pattern B:
  //   Drafter   → produces a draft text
  //   Critic    → receives Drafter's output as _workflow_state, evaluates it
  //   Publisher → receives Critic's feedback as _workflow_state, produces final copy
  //
  // Each stage re-runs the executor with the previous output seeded into
  // state.data["_workflow_state"] so the agent receives it as an assistant
  // message before the user query.
  private val threeStageWritingTest =
    test("three-stage pipeline: drafter output flows through critic into publisher") {
      val (store, executor) = freshEnv()

      val topic = "the importance of sleep for cognitive performance"

      val drafter = AgentNode(
        name         = "Drafter",
        model        = model,
        systemPrompt = "You are a content drafter. Write a 2-sentence paragraph on the given topic."
      )
      val critic = AgentNode(
        name         = "Critic",
        model        = model,
        systemPrompt =
          "You are a content critic. The 'Workflow state' in your context is a draft. " +
          "Identify one strength and one improvement for that draft. Be specific."
      )
      val publisher = AgentNode(
        name         = "Publisher",
        model        = model,
        systemPrompt =
          "You are a publisher. The 'Workflow state' contains a draft and its critique. " +
          "Produce an improved final version of the draft, incorporating the critique."
      )

      for {
        _           <- store.upsertNode(drafter)
        _           <- store.upsertNode(critic)
        _           <- store.upsertNode(publisher)

        // Stage 1: Drafter
        draftCtx    <- executor.run(drafter.nodeId, s"Write about: $topic")
        draftText    = agentText(draftCtx, drafter.nodeId)

        // Stage 2: Critic receives draft via _workflow_state
        criticCtx   <- executor.run(
                         critic.nodeId,
                         s"Critique the draft about: $topic",
                         state = Some(Map("_workflow_state" -> s"DRAFT:\n$draftText"))
                       )
        critiqueText = agentText(criticCtx, critic.nodeId)

        // Stage 3: Publisher receives draft + critique via _workflow_state
        publishCtx  <- executor.run(
                         publisher.nodeId,
                         s"Produce the final version about: $topic",
                         state = Some(Map(
                           "_workflow_state" ->
                             s"ORIGINAL DRAFT:\n$draftText\n\nCRITIQUE:\n$critiqueText"
                         ))
                       )
        finalText = agentText(publishCtx, publisher.nodeId)
      } yield {
        assertTrue(draftText.nonEmpty)    &&
        assertTrue(critiqueText.nonEmpty) &&
        assertTrue(finalText.nonEmpty)    &&
        // Each stage must have produced output
        assertTrue(draftCtx.outputs.contains(drafter.nodeId))    &&
        assertTrue(criticCtx.outputs.contains(critic.nodeId))    &&
        assertTrue(publishCtx.outputs.contains(publisher.nodeId))
      }
    }

  // ── Test 3: Parallel researchers → synthesiser ─────────────────────────────
  // Pattern C:
  //   Agent A researches topic X (space exploration)
  //   Agent B researches topic Y (deep-sea exploration) — independent run
  //   Both outputs are bridged as runtime ContextNodes linked to Synthesiser
  //   Synthesiser ingests both contexts and produces a comparative analysis
  private val parallelResearchersSynthesizerTest =
    test("two parallel researchers produce contexts consumed by a single synthesizer") {
      val (store, executor) = freshEnv()

      val researcherA = AgentNode(
        name         = "SpaceResearcher",
        model        = model,
        systemPrompt = "You are a space exploration expert. Summarise one key challenge in 2 sentences."
      )
      val researcherB = AgentNode(
        name         = "OceanResearcher",
        model        = model,
        systemPrompt = "You are a deep-sea exploration expert. Summarise one key challenge in 2 sentences."
      )
      val synthesizer = AgentNode(
        name         = "Synthesizer",
        model        = model,
        systemPrompt =
          "You are a comparative analyst. Under '## Relevant Context' you will find summaries " +
          "from two domain experts. Compare the challenges they describe and identify a common theme."
      )

      for {
        _           <- store.upsertNode(researcherA)
        _           <- store.upsertNode(researcherB)
        _           <- store.upsertNode(synthesizer)

        // Run both researchers independently (simulate parallel work)
        ctxA        <- executor.run(researcherA.nodeId, "What is the key challenge in space exploration?")
        ctxB        <- executor.run(researcherB.nodeId, "What is the key challenge in deep-sea exploration?")

        outputA = agentText(ctxA, researcherA.nodeId)
        outputB = agentText(ctxB, researcherB.nodeId)

        // Bridge both outputs as runtime contexts linked to the synthesizer
        // Use a shared session derived from the first run
        sharedSession = ctxA.sessionId
        _           <- bridgeAsContext(store, synthesizer.nodeId, outputA, "SpaceResearchFindings", sharedSession)
        _           <- bridgeAsContext(store, synthesizer.nodeId, outputB, "OceanResearchFindings", sharedSession)

        // Synthesizer runs with both contexts injected
        synthCtx    <- executor.run(
                         synthesizer.nodeId,
                         "Compare the challenges from both research domains and find a common theme.",
                         executionContext = Some(ctxA.copy(hopCount = 0, outputs = Map.empty))
                       )
        synthesis = agentText(synthCtx, synthesizer.nodeId)
      } yield {
        assertTrue(outputA.nonEmpty) &&
        assertTrue(outputB.nonEmpty) &&
        assertTrue(synthesis.nonEmpty) &&
        assertTrue(synthCtx.outputs.contains(synthesizer.nodeId))
      }
    }

  // ── Test 4: Iterative refinement — draft → critique → revision ─────────────
  // Pattern A + B combined:
  //   Agent A (Drafter) writes a short paragraph on a given topic.
  //   Agent B (Critic)  has A's draft injected as a runtime ContextNode AND
  //                     receives it as _workflow_state, giving it two avenues
  //                     to access the prior output.
  //   Agent C (Reviser) has both A's draft and B's critique as two separate
  //                     runtime ContextNodes (multi-context injection), and
  //                     produces a revised version that must improve on both.
  private val iterativeRefinementTest =
    test("iterative refinement: drafter → critic → reviser each build on prior agent context") {
      val (store, executor) = freshEnv()

      val topic = "renewable energy adoption challenges"

      val drafter = AgentNode(
        name         = "IterativeDrafter",
        model        = model,
        systemPrompt = "Write a concise 2-sentence paragraph on the given topic."
      )
      val critic = AgentNode(
        name         = "IterativeCritic",
        model        = model,
        systemPrompt =
          "You are a precise editor. You have a draft available in '## Relevant Context'. " +
          "List exactly: 1) one strength, 2) one specific weakness to fix."
      )
      val reviser = AgentNode(
        name         = "IterativeReviser",
        model        = model,
        systemPrompt =
          "You are a reviser. Under '## Relevant Context' you have the original draft and " +
          "a critique. Produce a revised paragraph that addresses the critique."
      )

      for {
        _          <- store.upsertNode(drafter)
        _          <- store.upsertNode(critic)
        _          <- store.upsertNode(reviser)

        // ── Stage 1: Draft ─────────────────────────────────────────────────
        draftCtx   <- executor.run(drafter.nodeId, s"Write about: $topic")
        draftText   = agentText(draftCtx, drafter.nodeId)

        // ── Stage 2: Critique — runtime context + _workflow_state (dual injection) ──
        _          <- bridgeAsContext(store, critic.nodeId, draftText, "OriginalDraft", draftCtx.sessionId)
        criticCtx  <- executor.run(
                        critic.nodeId,
                        s"Critique this draft about: $topic",
                        state            = Some(Map("_workflow_state" -> s"DRAFT:\n$draftText")),
                        executionContext = Some(draftCtx.copy(hopCount = 0, outputs = Map.empty))
                      )
        critiqueText = agentText(criticCtx, critic.nodeId)

        // ── Stage 3: Revise — two runtime contexts (draft + critique) ─────
        _          <- bridgeAsContext(store, reviser.nodeId, draftText,    "DraftForRevision",   draftCtx.sessionId)
        _          <- bridgeAsContext(store, reviser.nodeId, critiqueText, "CritiqueForRevision", draftCtx.sessionId)
        reviseCtx  <- executor.run(
                        reviser.nodeId,
                        s"Produce the revised version about: $topic",
                        executionContext = Some(draftCtx.copy(hopCount = 0, outputs = Map.empty))
                      )
        revisedText = agentText(reviseCtx, reviser.nodeId)
      } yield {
        assertTrue(draftText.nonEmpty)    &&
        assertTrue(critiqueText.nonEmpty) &&
        assertTrue(revisedText.nonEmpty)  &&
        assertTrue(draftCtx.outputs.contains(drafter.nodeId))   &&
        assertTrue(criticCtx.outputs.contains(critic.nodeId))   &&
        assertTrue(reviseCtx.outputs.contains(reviser.nodeId))  &&
        // Reviser had TWO context nodes — both should have been considered
        // (we verify structural correctness; content quality is the LLM's job)
        assert(reviseCtx.hopCount)(isGreaterThanEqualTo(1))
      }
    }

  // ── Test 5: Routing chain where each agent uses _last_output via state ──────
  // Pattern B (automatic _last_output threading):
  //   Three agents chained via routingTable.
  //   After each hop, _last_output is automatically stored in state.data.
  //   Agents' system prompts acknowledge the workflow state.
  //   Test verifies all three agents execute and each output is non-empty.
  private val routingChainContextTest =
    test("routing chain A→B→C: each agent executes and accumulates outputs in context") {
      val (store, executor) = freshEnv()

      val agentC = AgentNode(
        name         = "Presenter",
        model        = model,
        systemPrompt =
          "You are a presenter. Format the final answer from the workflow state as a clear conclusion.",
        routingTable = Map("default" -> "__END__")
      )
      val agentB = AgentNode(
        name         = "Evaluator",
        model        = model,
        systemPrompt =
          "You are an evaluator. Assess the quality of the analysis in the workflow state " +
          "and give a brief recommendation.",
        routingTable = Map("default" -> agentC.nodeId)
      )
      val agentA = AgentNode(
        name         = "Analyzer",
        model        = model,
        systemPrompt = "You are an analyst. Briefly analyse the given question.",
        routingTable = Map("default" -> agentB.nodeId)
      )

      for {
        _ <- store.upsertNode(agentA)
        _ <- store.upsertNode(agentB)
        _ <- store.upsertNode(agentC)

        // Run the full chain from A; _last_output is threaded automatically
        ctx <- executor.run(
                 agentA.nodeId,
                 "What are the trade-offs between microservices and monolithic architectures?"
               )
        explain = explainRun(ctx)
      } yield {
        // All three agents must have run
        assert(ctx.hopCount)(isGreaterThanEqualTo(3))                      &&
        assertTrue(ctx.outputs.contains(agentA.nodeId))                    &&
        assertTrue(ctx.outputs.contains(agentB.nodeId))                    &&
        assertTrue(ctx.outputs.contains(agentC.nodeId))                    &&
        assertTrue(agentText(ctx, agentA.nodeId).nonEmpty)                 &&
        assertTrue(agentText(ctx, agentB.nodeId).nonEmpty)                 &&
        assertTrue(agentText(ctx, agentC.nodeId).nonEmpty)                 &&
        // At least 2 routing decisions: A→B and B→C
        assert(explain.summary.routingDecisionCount)(isGreaterThanEqualTo(2))
      }
    }

  // ── suite ──────────────────────────────────────────────────────────────────

  def spec =
    suite("MultiAgentContextTests")(
      extractorAnalystTest,
      threeStageWritingTest,
      parallelResearchersSynthesizerTest,
      iterativeRefinementTest,
      routingChainContextTest
    ) @@ TestAspect.timeout(5.minutes) @@ TestAspect.sequential
}
