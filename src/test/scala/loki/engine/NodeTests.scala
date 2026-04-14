package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.core.executor.*
import loki.engine.backend.*
import loki.engine.app.*

object NodeTests {

  def spec = suite("NodeTests")(
    test("AgentNode should have correct defaults") {
      val agent = AgentNode(name = "TestAgent")
      assert(agent.nodeType)(Assertion.equalTo(NodeType.Agent)) &&
      assert(agent.name)(Assertion.equalTo("TestAgent")) &&
      assert(agent.maxIterations)(Assertion.equalTo(10)) &&
      assert(agent.maxTokens)(Assertion.equalTo(8096)) &&
      assert(agent.model)(Assertion.equalTo("claude-sonnet-4-6")) &&
      assert(agent.routingTable("default"))(Assertion.equalTo("__END__"))
    },
    
    test("ToolNode should convert to tool schema") {
      val tool = ToolNode(
        name = "search",
        description = "Search tool",
        callableRef = "test.search",
        inputSchema = Map(
          "type" -> "object",
          "properties" -> Map("query" -> Map("type" -> "string")),
          "required" -> List("query")
        )
      )
      val schema = tool.toToolSchema
      assert(schema("name"))(Assertion.equalTo("search")) &&
      assert(schema("description"))(Assertion.equalTo("Search tool"))
    },
    
    test("ContextNode should check fact validity") {
      val ctx = ContextNode(
        name = "TestContext",
        content = "Test content"
      )
      assert(ctx.isValid)(Assertion.equalTo(true)) &&
      assert(ctx.isFactValid)(Assertion.equalTo(true))
    },
    
    test("ContextNode with past fact_invalid_at should not be fact valid") {
      val ctx = ContextNode(
        name = "ExpiredContext",
        content = "Old content",
        factInvalidAt = Some(java.time.Instant.now().minusSeconds(3600))
      )
      assert(ctx.isFactValid)(Assertion.equalTo(false))
    },
    
    test("PromptNode should render template") {
      val prompt = PromptNode(
        name = "Greeting",
        template = "Hello {{name}}! You are {{age}} years old."
      )
      val rendered = prompt.render(Map("name" -> "Alice", "age" -> "30"))
      assert(rendered)(Assertion.equalTo("Hello Alice! You are 30 years old."))
    },
    
    test("Node expire should set invalid_at") {
      val agent = AgentNode(name = "ExpiringAgent")
      val expired = agent.expire()
      assert(expired.isValid)(Assertion.equalTo(false))
    },
    
    test("ApprovalNode should have correct defaults") {
      val approval = ApprovalNode(
        name = "TestApproval",
        instructions = "Please approve"
      )
      assert(approval.nodeType)(Assertion.equalTo(NodeType.Approval)) &&
      assert(approval.assignmentMode)(Assertion.equalTo("any")) &&
      assert(approval.inputKey)(Assertion.equalTo("approval"))
    },
    
    test("AnyNode.fromMap should deserialize AgentNode") {
      val data = Map(
        "node_type" -> "agent",
        "node_id" -> "test-id",
        "name" -> "FromMapAgent",
        "model" -> "claude-3"
      )
      val node = AnyNode.fromMap(data)
      node match {
        case a: AgentNode =>
          assert(a.nodeId)(Assertion.equalTo("test-id")) &&
          assert(a.name)(Assertion.equalTo("FromMapAgent"))
        case _ => assert(true)(Assertion.equalTo(false))
      }
    },

    test("TransformNode should have correct defaults") {
      val t = TransformNode(name = "reshaper", callableRef = "fn.reshape")
      assert(t.nodeType)(Assertion.equalTo(NodeType.Transform)) &&
      assert(t.callableRef)(Assertion.equalTo("fn.reshape")) &&
      assert(t.inputKeys)(Assertion.equalTo(Nil)) &&
      assert(t.outputKey)(Assertion.equalTo("")) &&
      assert(t.isAsync)(Assertion.equalTo(true))
    },

    test("TransformNode expire should set invalidAt") {
      val t = TransformNode(name = "expiring")
      val expired = t.expire()
      assert(expired.isValid)(Assertion.equalTo(false))
    },

    test("AnyNode.fromMap should deserialize TransformNode") {
      val data = Map(
        "node_type" -> "transform",
        "node_id" -> "tx-1",
        "name" -> "joiner",
        "callable_ref" -> "fn.join",
        "input_keys" -> List("worker_a", "worker_b"),
        "output_key" -> "joined",
        "is_async" -> "false"
      )
      val node = AnyNode.fromMap(data)
      node match {
        case t: TransformNode =>
          assert(t.nodeId)(Assertion.equalTo("tx-1")) &&
          assert(t.callableRef)(Assertion.equalTo("fn.join")) &&
          assert(t.inputKeys)(Assertion.equalTo(List("worker_a", "worker_b"))) &&
          assert(t.outputKey)(Assertion.equalTo("joined")) &&
          assert(t.isAsync)(Assertion.equalTo(false))
        case _ => assert(true)(Assertion.equalTo(false))
      }
    }
  )
}
