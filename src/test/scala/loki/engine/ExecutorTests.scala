package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.core.executor.*
import loki.engine.backend.*

object ExecutorTests {

  class SequencedBackend(responses: List[LLMResponse]) extends LLMBackend {
    private val index = new java.util.concurrent.atomic.AtomicInteger(0)
    val maxTokensSeen = new java.util.concurrent.atomic.AtomicReference[List[Int]](Nil)

    def chat(
      model: String,
      system: String,
      messages: List[Map[String, Any]],
      tools: List[Map[String, Any]],
      maxTokens: Int = 8096
    ): Task[LLMResponse] = ZIO.succeed {
      maxTokensSeen.updateAndGet(_ :+ maxTokens)
      val response = responses(math.min(index.get, responses.length - 1))
      index.incrementAndGet()
      response
    }

    def extendMessages(
      messages: List[Map[String, Any]],
      response: LLMResponse,
      toolResults: List[ToolResult]
    ): List[Map[String, Any]] = messages :+ Map("role" -> "assistant", "content" -> response.text)
  }

  class MockLLMBackend extends LLMBackend {
    def chat(
      model: String,
      system: String,
      messages: List[Map[String, Any]],
      tools: List[Map[String, Any]],
      maxTokens: Int = 8096
    ): Task[LLMResponse] = ZIO.succeed {
      LLMResponse(
        text = "Mock response",
        toolCalls = Nil,
        stopReason = "end_turn"
      )
    }
    
    def extendMessages(
      messages: List[Map[String, Any]],
      response: LLMResponse,
      toolResults: List[ToolResult]
    ): List[Map[String, Any]] = messages
  }
  
  class MockToolFn extends (Any => Any) {
    def apply(input: Any): Any = s"Tool executed: $input"
  }

  def spec = suite("ExecutorTests")(
    test("GraphExecutor should execute agent node") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)
      
      val agent = AgentNode(name = "TestAgent", systemPrompt = "You are a test agent.")
      
      for {
        _ <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "Test query")
      } yield {
        assert(ctx.sessionId.nonEmpty)(Assertion.equalTo(true)) &&
        assert(ctx.hopCount)(Assertion.equalTo(1))
      }
    },

    test("token-truncated agent turn escalates and completes") {
      val store = new InMemoryGraphStore()
      val backend = new SequencedBackend(List(
        LLMResponse("", Nil, "max_tokens", reasoning = "budget consumed by reasoning"),
        LLMResponse("DONE", Nil, "end_turn")
      ))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "ReasoningAgent")

      for {
        _ <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "go")
      } yield {
        val output = ctx.outputs(agent.nodeId).asInstanceOf[Map[String, Any]]
        assert(output("text"))(Assertion.equalTo("DONE")) &&
        assert(backend.maxTokensSeen.get())(Assertion.equalTo(List(8096, 16192)))
      }
    },

    test("persistent token truncation is bounded and salvages reasoning") {
      val store = new InMemoryGraphStore()
      val truncated = LLMResponse("", Nil, "max_tokens", reasoning = "all in the head")
      val backend = new SequencedBackend(List(truncated, truncated, truncated))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "ReasoningAgent")

      for {
        _ <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "go")
      } yield {
        val output = ctx.outputs(agent.nodeId).asInstanceOf[Map[String, Any]]
        assert(output("text"))(Assertion.equalTo("all in the head")) &&
        assert(backend.maxTokensSeen.get())(Assertion.equalTo(List(8096, 16192, 32000)))
      }
    },

    test("empty end turn salvages reasoning") {
      val store = new InMemoryGraphStore()
      val backend = new SequencedBackend(List(
        LLMResponse("", Nil, "end_turn", reasoning = "visible answer from reasoning")
      ))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "ReasoningAgent")

      for {
        _ <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "go")
      } yield {
        val output = ctx.outputs(agent.nodeId).asInstanceOf[Map[String, Any]]
        assert(output("text"))(Assertion.equalTo("visible answer from reasoning"))
      }
    },
    
    test("GraphExecutor should execute context node") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)
      
      val ctx = ContextNode(name = "TestContext", content = "Test content here.")
      
      for {
        _ <- store.upsertNode(ctx)
        resultCtx <- executor.run(ctx.nodeId, "Test query")
      } yield {
        assert(resultCtx.outputs.contains(ctx.nodeId))(Assertion.equalTo(true)) &&
        assert(resultCtx.outputs(ctx.nodeId))(Assertion.equalTo("Test content here."))
      }
    },
    
    test("GraphExecutor should register and execute tool") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)
      
      val tool = ToolNode(name = "test_tool", callableRef = "test.fn")
      executor.registerTool("test.fn", new MockToolFn())
      
      for {
        _ <- store.upsertNode(tool)
        ctx <- executor.run(tool.nodeId, "Test input")
      } yield {
        assert(ctx.outputs.contains(tool.nodeId))(Assertion.equalTo(true))
      }
    },
    
    test("GraphExecutor should track outputs") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)
      
      val ctx = ContextNode(name = "OutputTest", content = "Output content")
      
      for {
        _ <- store.upsertNode(ctx)
        resultCtx <- executor.run(ctx.nodeId, "Test")
      } yield {
        assert(resultCtx.outputs.nonEmpty)(Assertion.equalTo(true))
      }
    },
    
    test("GraphExecutor should respect maxHops") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer, routerModel = "mock")
      
      val agent = AgentNode(name = "MaxHopAgent", maxIterations = 1)
      
      for {
        _ <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "Test", maxHops = 2)
      } yield {
        assert(ctx.maxHops)(Assertion.equalTo(2))
      }
    },
    
    test("zero maxIterations exhausts before the first turn, not with an error") {
      // A max_iterations<=0 node must return the exhausted result, not raise:
      // the guard fires on loop entry, the agent ends normally, and not one
      // LLM turn is spent.
      val store = new InMemoryGraphStore()
      val backend = new SequencedBackend(List(LLMResponse("unreachable", Nil, "end_turn")))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "ZeroIterationAgent", maxIterations = 0)

      for {
        _ <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "go")
      } yield {
        val output = ctx.outputs(agent.nodeId).asInstanceOf[Map[String, Any]]
        assert(output("intent"))(Assertion.equalTo("default")) &&
        assert(output("text"))(Assertion.equalTo("go")) &&
        assert(backend.maxTokensSeen.get())(Assertion.equalTo(Nil))
      }
    },

    test("GraphExecutor should use provided execution context") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)
      
      val ctx = ContextNode(name = "StateTest", content = "Content")
      val execCtx = ExecutionContext(query = "Query", maxHops = 5)
      
      for {
        _ <- store.upsertNode(ctx)
        resultCtx <- executor.run(ctx.nodeId, "Query", executionContext = Some(execCtx))
      } yield {
        assert(resultCtx.sessionId)(Assertion.equalTo(execCtx.sessionId))
      }
    },
    
    test("GraphExecutor should route to next agent") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)
      
      val agent1 = AgentNode(name = "Agent1", routingTable = Map("default" -> "__END__"))
      val ctx = ContextNode(name = "Ctx", content = "Content")
      
      for {
        _ <- store.upsertNode(agent1)
        _ <- store.upsertNode(ctx)
        resultCtx <- executor.run(agent1.nodeId, "Test")
      } yield {
        assert(resultCtx.trace.nonEmpty)(Assertion.equalTo(true))
      }
    },
    
    test("GraphExecutor should emit trace events") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)

      val agent = AgentNode(name = "TraceAgent")

      for {
        _ <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "Test")
      } yield {
        assert(ctx.trace.map(_.eventType))(Assertion.contains(EventType.Hop)) &&
        assert(ctx.trace.map(_.eventType))(Assertion.contains(EventType.AgentStart)) &&
        assert(ctx.trace.map(_.eventType))(Assertion.contains(EventType.AgentEnd)) &&
        assert(ctx.trace.map(_.eventType))(Assertion.contains(EventType.Routing))
      }
    },

    test("TransformNode should execute registered callable") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)

      val transform = TransformNode(name = "doubler", callableRef = "fn.double")
      executor.registerTool("fn.double", (input: Any) => {
        val m = input.asInstanceOf[Map[String, Any]]
        Map("value" -> (m.getOrElse("value", 0).toString.toInt * 2))
      })

      for {
        _ <- store.upsertNode(transform)
        ctx <- executor.run(transform.nodeId, "test", state = Some(Map("value" -> 21)))
      } yield {
        assert(ctx.outputs.contains(transform.nodeId))(Assertion.equalTo(true))
      }
    },

    test("TransformNode should fail when callable is not registered") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)

      val transform = TransformNode(name = "missing", callableRef = "fn.missing")

      for {
        _      <- store.upsertNode(transform)
        result <- executor.run(transform.nodeId, "test").exit
      } yield {
        assert(result.isFailure)(Assertion.equalTo(true))
      }
    },

    test("TransformNode with outputKey writes result to state.data") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)

      val transform = TransformNode(name = "writer", callableRef = "fn.write", outputKey = "written")
      executor.registerTool("fn.write", (_: Any) => Map("result" -> "ok"))

      for {
        _ <- store.upsertNode(transform)
        ctx <- executor.run(transform.nodeId, "test")
      } yield {
        assert(ctx.state.data.contains("written"))(Assertion.equalTo(true))
      }
    },

    test("TransformNode with inputKeys collects outputs from prior nodes") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val composer = new AgentComposer(store)
      val executor = new GraphExecutor(store, backend, composer)

      val ctx1 = ContextNode(nodeId = "ctx-a", name = "A", content = "hello")
      val transform = TransformNode(
        name = "joiner",
        callableRef = "fn.join",
        inputKeys = List("ctx-a"),
        outputKey = "merged"
      )
      executor.registerTool("fn.join", (input: Any) => {
        val m = input.asInstanceOf[Map[String, Any]]
        Map("merged" -> m.values.mkString("|"))
      })

      for {
        _ <- store.upsertNode(ctx1)
        _ <- store.upsertNode(transform)
        ctx <- executor.run(transform.nodeId, "test",
          executionContext = Some(ExecutionContext(
            query = "test",
            outputs = Map("ctx-a" -> "hello")
          ))
        )
      } yield {
        assert(ctx.state.data.contains("merged"))(Assertion.equalTo(true))
      }
    }
  )
}
