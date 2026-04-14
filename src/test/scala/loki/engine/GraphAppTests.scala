package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.core.executor.*
import loki.engine.backend.*
import loki.engine.app.*

object GraphAppTests {

  class MockLLMBackend extends LLMBackend {
    def chat(
      model: String,
      system: String,
      messages: List[Map[String, Any]],
      tools: List[Map[String, Any]],
      maxTokens: Int = 8096
    ): Task[LLMResponse] = ZIO.succeed {
      LLMResponse(text = "Mock response", toolCalls = Nil, stopReason = "end_turn")
    }
    
    def extendMessages(
      messages: List[Map[String, Any]],
      response: LLMResponse,
      toolResults: List[ToolResult]
    ): List[Map[String, Any]] = messages
  }

  def spec = suite("GraphAppTests")(
    test("GraphApp should add agent") {
      val store = new InMemoryGraphStore()
      val app = new GraphApp(store)
      val agent = createAgent(name = "TestAgent")
      
      for {
        result <- app.addAgent("TestAgent", agent)
      } yield {
        assert(result.name)(Assertion.equalTo("TestAgent")) &&
        assert(result.nodeType)(Assertion.equalTo(NodeType.Agent))
      }
    },
    
    test("GraphApp should add tool") {
      val store = new InMemoryGraphStore()
      val app = new GraphApp(store)
      val tool = createTool(name = "TestTool", callableRef = "test.fn")
      
      for {
        result <- app.addTool("TestTool", tool)
      } yield {
        assert(result.name)(Assertion.equalTo("TestTool"))
      }
    },
    
    test("GraphApp should add context") {
      val store = new InMemoryGraphStore()
      val app = new GraphApp(store)
      val ctx = createContext(name = "TestContext", content = "Test content")
      
      for {
        result <- app.addContext("TestContext", ctx)
      } yield {
        assert(result.name)(Assertion.equalTo("TestContext")) &&
        assert(result.content)(Assertion.equalTo("Test content"))
      }
    },
    
    test("GraphApp should add prompt") {
      val store = new InMemoryGraphStore()
      val app = new GraphApp(store)
      val prompt = createPrompt(name = "TestPrompt", template = "Hello {{name}}")
      
      for {
        result <- app.addPrompt("TestPrompt", prompt)
      } yield {
        assert(result.name)(Assertion.equalTo("TestPrompt"))
      }
    },
    
    test("GraphApp should register tool") {
      val store = new InMemoryGraphStore()
      val app = new GraphApp(store)
      
      app.registerTool("test.fn", (input: Any) => s"Result: $input")
      
      val executor = app.getExecutor
      assert(executor != null)(Assertion.equalTo(true))
    },
    
    test("GraphApp should connect tool to agent") {
      val store = new InMemoryGraphStore()
      val app = new GraphApp(store)
      val agent = createAgent(name = "Agent")
      val tool = createTool(name = "Tool", callableRef = "test.fn")
      
      for {
        _ <- app.addAgent("Agent", agent)
        _ <- app.addTool("Tool", tool)
        edge <- app.connectTool(agent.nodeId, tool.nodeId)
      } yield {
        assert(edge.edgeType)(Assertion.equalTo(EdgeType.HAS_TOOL)) &&
        assert(edge.srcId)(Assertion.equalTo(agent.nodeId)) &&
        assert(edge.dstId)(Assertion.equalTo(tool.nodeId))
      }
    },
    
    test("GraphApp should connect context to agent") {
      val store = new InMemoryGraphStore()
      val app = new GraphApp(store)
      val agent = createAgent(name = "Agent")
      val ctx = createContext(name = "Context", content = "Content")
      
      for {
        _ <- app.addAgent("Agent", agent)
        _ <- app.addContext("Context", ctx)
        edge <- app.connectContext(agent.nodeId, ctx.nodeId, weight = Some(0.8))
      } yield {
        assert(edge.edgeType)(Assertion.equalTo(EdgeType.HAS_CONTEXT)) &&
        assert(edge.weight)(Assertion.equalTo(0.8))
      }
    },
    
    test("GraphApp should delegate between agents") {
      val store = new InMemoryGraphStore()
      val app = new GraphApp(store)
      val agent1 = createAgent(name = "Agent1")
      val agent2 = createAgent(name = "Agent2")
      
      for {
        _ <- app.addAgent("Agent1", agent1)
        _ <- app.addAgent("Agent2", agent2)
        edge <- app.delegate(agent1.nodeId, agent2.nodeId)
      } yield {
        assert(edge.edgeType)(Assertion.equalTo(EdgeType.DELEGATES_TO))
      }
    },
    
    test("GraphApp should get executor lazily") {
      val store = new InMemoryGraphStore()
      val backend = new MockLLMBackend()
      val app = new GraphApp(store, backend = Some(backend))
      
      val executor1 = app.getExecutor
      val executor2 = app.getExecutor
      
      assert(executor1 == executor2)(Assertion.equalTo(true))
    },
    
    test("createAgent should set defaults") {
      val agent = createAgent(name = "CreateTest", model = "claude-3")
      assert(agent.name)(Assertion.equalTo("CreateTest")) &&
      assert(agent.model)(Assertion.equalTo("claude-3")) &&
      assert(agent.routingTable("default"))(Assertion.equalTo("__END__"))
    },
    
    test("createTool should set defaults") {
      val tool = createTool(name = "CreateTool", callableRef = "test.fn")
      assert(tool.name)(Assertion.equalTo("CreateTool")) &&
      assert(tool.isAsync)(Assertion.equalTo(true))
    },
    
    test("createContext should set content") {
      val ctx = createContext(name = "CreateCtx", content = "Test content")
      assert(ctx.name)(Assertion.equalTo("CreateCtx")) &&
      assert(ctx.content)(Assertion.equalTo("Test content"))
    }
  )
}