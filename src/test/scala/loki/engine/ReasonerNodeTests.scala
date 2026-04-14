package loki.engine

import zio._
import zio.test.{test, assert, Assertion, Spec, suite}
import loki.engine.core.nodes._
import loki.engine.core.edges._
import loki.engine.core.store._
import loki.engine.core.executor._
import loki.engine.backend._
import loki.engine.symbolic._

object ReasonerNodeTests {

  private class MockLLMBackend extends LLMBackend {
    def chat(
      model: String,
      system: String,
      messages: List[Map[String, Any]],
      tools: List[Map[String, Any]],
      maxTokens: Int = 8096
    ): Task[LLMResponse] = ZIO.succeed(
      LLMResponse(text = "Mock", toolCalls = Nil, stopReason = "end_turn")
    )
    def extendMessages(
      messages: List[Map[String, Any]],
      response: LLMResponse,
      toolResults: List[ToolResult]
    ): List[Map[String, Any]] = messages
  }

  private def makeExecutor(store: GraphStore = new InMemoryGraphStore()): GraphExecutor = {
    val backend  = new MockLLMBackend()
    val composer = new AgentComposer(store)
    new GraphExecutor(store, backend, composer)
  }

  def spec = suite("ReasonerNodeTests")(

    // -----------------------------------------------------------------------
    // Node construction
    // -----------------------------------------------------------------------

    test("ReasonerNode has correct defaults") {
      val n = ReasonerNode(name = "classifier")
      ZIO.succeed(
        assert(n.nodeType)(Assertion.equalTo(NodeType.Reasoner)) &&
        assert(n.emitDerivedOnly)(Assertion.isTrue) &&
        assert(n.withProof)(Assertion.isFalse) &&
        assert(n.outputKey)(Assertion.equalTo("inferred")) &&
        assert(n.failOnEmpty)(Assertion.isFalse)
      )
    },

    test("ReasonerNode expiry sets invalidAt") {
      val n       = ReasonerNode(name = "r")
      val expired = n.expire().asInstanceOf[ReasonerNode]
      ZIO.succeed(assert(expired.invalidAt.isDefined)(Assertion.isTrue))
    },

    test("AnyNode.fromMap reconstructs a ReasonerNode") {
      val data = Map(
        "node_type"     -> "reasoner",
        "node_id"       -> "r1",
        "name"          -> "deduce",
        "program"       -> "safe(X) :- node(X), !bad(X).",
        "output_key"    -> "safe_nodes",
        "with_proof"    -> "true",
        "fail_on_empty" -> "false"
      )
      ZIO.succeed(AnyNode.fromMap(data)).map {
        case r: ReasonerNode =>
          assert(r.nodeId)(Assertion.equalTo("r1")) &&
          assert(r.name)(Assertion.equalTo("deduce")) &&
          assert(r.outputKey)(Assertion.equalTo("safe_nodes")) &&
          assert(r.withProof)(Assertion.isTrue)
        case _ => assert(false)(Assertion.isTrue)
      }
    },

    test("FactSource defaults are sensible") {
      val fs = FactSource()
      ZIO.succeed(
        assert(fs.stateKeys)(Assertion.isEmpty) &&
        assert(fs.edgeTypes)(Assertion.isEmpty) &&
        assert(fs.includeNodeFacts)(Assertion.isFalse) &&
        assert(fs.useNodeNames)(Assertion.isTrue)
      )
    },

    // -----------------------------------------------------------------------
    // executeReasoner via run()
    // -----------------------------------------------------------------------

    test("executeReasoner derives facts from state") {
      val store  = new InMemoryGraphStore()
      val exec   = makeExecutor(store)
      val node   = ReasonerNode(
        nodeId     = "r1",
        name       = "family",
        program    = "ancestor(X,Z) :- parent(X,Y), parent(Y,Z).",
        factSource = FactSource(stateKeys = List("facts")),
        outputKey  = "inferred"
      )
      val initialFacts: List[Any] = List(
        List("parent", "alice", "bob"),
        List("parent", "bob",   "carol")
      )
      val ctx = ExecutionContext(
        query = "test",
        state = WorkflowState(data = Map("facts" -> initialFacts))
      )
      for {
        _       <- store.upsertNode(node)
        newCtx  <- exec.run("r1", "test", executionContext = Some(ctx))
        out      = newCtx.outputs.get("r1")
          .orElse(newCtx.state.data.get("inferred"))
        hasFacts = out match {
          case Some(m: Map[_, ?]) =>
            m.asInstanceOf[Map[String, Any]].get("facts") match {
              case Some(l: List[?]) => l.nonEmpty
              case _                => false
            }
          case _ => false
        }
      } yield assert(hasFacts)(Assertion.isTrue)
    },

    test("executeReasoner writes outputKey to state.data") {
      val store = new InMemoryGraphStore()
      val exec  = makeExecutor(store)
      val node  = ReasonerNode(
        nodeId     = "r2",
        name       = "tagger",
        program    = "tagged(X) :- item(X).",
        factSource = FactSource(stateKeys = List("items")),
        outputKey  = "tagged_items"
      )
      val ctx = ExecutionContext(
        query = "test",
        state = WorkflowState(data = Map("items" -> List(List("item", "widget"))))
      )
      for {
        _      <- store.upsertNode(node)
        newCtx <- exec.run("r2", "test", executionContext = Some(ctx))
      } yield assert(newCtx.state.data.contains("tagged_items"))(Assertion.isTrue)
    },

    test("failOnEmpty raises an error when no derived facts") {
      val store = new InMemoryGraphStore()
      val exec  = makeExecutor(store)
      val node  = ReasonerNode(
        nodeId      = "r3",
        name        = "strict",
        program     = "result(X) :- base(X).",
        factSource  = FactSource(stateKeys = List("facts")),
        query       = List("result"),
        failOnEmpty = true,
        outputKey   = "out"
      )
      val ctx = ExecutionContext(
        query = "test",
        state = WorkflowState(data = Map("facts" -> List.empty))
      )
      for {
        _      <- store.upsertNode(node)
        failed <- exec.run("r3", "test", executionContext = Some(ctx)).fold(_ => true, _ => false)
      } yield assert(failed)(Assertion.isTrue)
    },

    test("withProof populates proofs in output") {
      val store = new InMemoryGraphStore()
      val exec  = makeExecutor(store)
      val node  = ReasonerNode(
        nodeId     = "r4",
        name       = "prover",
        program    = "large(X) :- size(X, big).",
        factSource = FactSource(stateKeys = List("facts")),
        withProof  = true,
        outputKey  = "proved"
      )
      val ctx = ExecutionContext(
        query = "test",
        state = WorkflowState(data = Map("facts" -> List(List("size", "elephant", "big"))))
      )
      for {
        _      <- store.upsertNode(node)
        newCtx <- exec.run("r4", "test", executionContext = Some(ctx))
        out     = newCtx.outputs.get("r4")
        hasProofs = out match {
          case Some(m: Map[_, ?]) => m.asInstanceOf[Map[String, Any]].contains("proofs")
          case _                  => false
        }
      } yield assert(hasProofs)(Assertion.isTrue)
    },

    test("emitDerivedOnly=false includes input facts in output") {
      val store = new InMemoryGraphStore()
      val exec  = makeExecutor(store)
      val node  = ReasonerNode(
        nodeId          = "r5",
        name            = "all-facts",
        program         = "derived(X) :- base(X).",
        factSource      = FactSource(stateKeys = List("facts")),
        emitDerivedOnly = false,
        outputKey       = "all"
      )
      val ctx = ExecutionContext(
        query = "test",
        state = WorkflowState(data = Map("facts" -> List(List("base", "x"))))
      )
      for {
        _      <- store.upsertNode(node)
        newCtx <- exec.run("r5", "test", executionContext = Some(ctx))
        out     = newCtx.outputs.get("r5")
        inputPresent = out match {
          case Some(m: Map[_, ?]) =>
            m.asInstanceOf[Map[String, Any]].get("input_count") match {
              case Some(n: Int) => n > 0
              case _            => false
            }
          case _ => false
        }
      } yield assert(inputPresent)(Assertion.isTrue)
    },

    // -----------------------------------------------------------------------
    // inspectResume
    // -----------------------------------------------------------------------

    test("inspectResume reports ok for a fresh running context") {
      val exec = makeExecutor()
      val ctx  = ExecutionContext(query = "test")
      val r    = exec.inspectResume(ctx)
      ZIO.succeed(
        assert(r.ok)(Assertion.isTrue) &&
        assert(r.isStale)(Assertion.isFalse) &&
        assert(r.unrecoverable)(Assertion.isEmpty)
      )
    },

    test("inspectResume detects missing required outputs") {
      val exec = makeExecutor()
      val ctx  = ExecutionContext(query = "test")
      val r    = exec.inspectResume(ctx, requiredOutputs = List("missing_key"))
      ZIO.succeed(assert(r.unrecoverable)(Assertion.contains("missing_key")))
    },

    test("inspectResume finds outputs in ctx.state.data") {
      val exec = makeExecutor()
      val ctx  = ExecutionContext(
        query = "test",
        state = WorkflowState(data = Map("my_key" -> "some_value"))
      )
      val r = exec.inspectResume(ctx, requiredOutputs = List("my_key"))
      ZIO.succeed(
        assert(r.available)(Assertion.contains("my_key")) &&
        assert(r.unrecoverable)(Assertion.isEmpty)
      )
    },

    test("inspectResume marks stale context") {
      val exec     = makeExecutor()
      val oldEvent = TraceEvent(
        eventType = EventType.Hop,
        sessionId = "s1",
        nodeId    = "n1",
        nodeName  = "test",
        timestamp = java.time.Instant.now().minusSeconds(3600)
      )
      val ctx = ExecutionContext(query = "test", trace = List(oldEvent))
      val r   = exec.inspectResume(ctx, staleAfterSeconds = 60.0)
      ZIO.succeed(
        assert(r.isStale)(Assertion.isTrue) &&
        assert(r.ok)(Assertion.isFalse)
      )
    },

    // -----------------------------------------------------------------------
    // Checkpoint round-trip
    // -----------------------------------------------------------------------

    test("checkpointContext + loadCheckpoint round-trips the context") {
      val store = new InMemoryGraphStore()
      val exec  = makeExecutor(store)
      val ctx   = ExecutionContext(
        query   = "round-trip test",
        outputs = Map("step1" -> Map("value" -> 42)),
        state   = WorkflowState(data = Map("counter" -> 7), status = "paused")
      )
      for {
        checkpoint <- exec.checkpointContext(ctx, "test-checkpoint")
        restored   <- exec.loadCheckpoint(checkpoint.nodeId)
      } yield
        assert(restored.query)(Assertion.equalTo("round-trip test")) &&
        assert(restored.state.status)(Assertion.equalTo("paused")) &&
        assert(restored.state.data.get("counter"))(Assertion.equalTo(Some(7)))
    },

    // -----------------------------------------------------------------------
    // materialiseOutput
    // -----------------------------------------------------------------------

    test("materialiseOutput creates a ContextNode in the store") {
      val store   = new InMemoryGraphStore()
      val exec    = makeExecutor(store)
      val srcNode = AgentNode(nodeId = "agent1", name = "writer")
      val ctx     = ExecutionContext(query = "test")
      for {
        _       <- store.upsertNode(srcNode)
        ctxNode <- exec.materialiseOutput(srcNode, "Hello world", ctx)
        found   <- store.getNode(ctxNode.nodeId)
      } yield found match {
        case Some(n: ContextNode) =>
          assert(n.content)(Assertion.equalTo("Hello world")) &&
          assert(n.source)(Assertion.equalTo("node:agent1"))
        case _ => assert(false)(Assertion.isTrue)
      }
    },

    // -----------------------------------------------------------------------
    // ExecutionContext.fromSnapshot
    // -----------------------------------------------------------------------

    test("ExecutionContext.fromSnapshot reconstructs a context") {
      val ctx = ExecutionContext(
        query     = "snapshot test",
        sessionId = "sess-abc",
        state     = WorkflowState(status = "paused", data = Map("k" -> "v"))
      )
      val restored = ExecutionContext.fromSnapshot(ctx.snapshot)
      ZIO.succeed(
        assert(restored.query)(Assertion.equalTo("snapshot test")) &&
        assert(restored.sessionId)(Assertion.equalTo("sess-abc")) &&
        assert(restored.state.status)(Assertion.equalTo("paused")) &&
        assert(restored.state.data.get("k"))(Assertion.equalTo(Some("v")))
      )
    }
  )
}
