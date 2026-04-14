package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.executor.*
import loki.engine.observability.*

object ObservabilityTests {

  def spec = suite("ObservabilityTests")(
    test("extractTraceView should categorize events") {
      val events = List(
        TraceEvent(EventType.Hop, "s1", "n1", "Node1", payload = Map("hop" -> 1)),
        TraceEvent(EventType.AgentStart, "s1", "n1", "Agent1", payload = Map()),
        TraceEvent(EventType.AgentEnd, "s1", "n1", "Agent1", payload = Map("text_summary" -> "done")),
        TraceEvent(EventType.ToolCall, "s1", "n1", "Tool1", payload = Map()),
        TraceEvent(EventType.ToolResult, "s1", "n1", "Tool1", payload = Map("success" -> true)),
        TraceEvent(EventType.Routing, "s1", "n1", "Router", payload = Map("intent" -> "next"))
      )
      
      val view = extractTraceView(events)
      
      assert(view.hops.length)(Assertion.equalTo(1)) &&
      assert(view.agentStarts.length)(Assertion.equalTo(1)) &&
      assert(view.agentEnds.length)(Assertion.equalTo(1)) &&
      assert(view.toolCalls.length)(Assertion.equalTo(1)) &&
      assert(view.toolResults.length)(Assertion.equalTo(1)) &&
      assert(view.routing.length)(Assertion.equalTo(1))
    },
    
    test("explainRun should create summary") {
      val ctx = ExecutionContext(
        query = "Test query",
        sessionId = "test-session",
        hopCount = 3,
        trace = List(
          TraceEvent(EventType.Hop, "test-session", "n1", "Node1", payload = Map("hop" -> 1)),
          TraceEvent(EventType.Routing, "test-session", "n1", "Router", payload = Map("intent" -> "search", "mode" -> "llm")),
          TraceEvent(EventType.ToolCall, "test-session", "n1", "Tool1", payload = Map()),
          TraceEvent(EventType.ToolResult, "test-session", "n1", "Tool1", payload = Map("success" -> true)),
          TraceEvent(EventType.AgentEnd, "test-session", "n1", "Agent1", payload = Map("text_summary" -> "done", "intent" -> "search"))
        ),
        state = WorkflowState()
      )
      
      val explanation = explainRun(ctx)
      
      assert(explanation.sessionId)(Assertion.equalTo("test-session")) &&
      assert(explanation.hopCount)(Assertion.equalTo(3)) &&
      assert(explanation.summary.toolCallCount)(Assertion.equalTo(1)) &&
      assert(explanation.summary.routingDecisionCount)(Assertion.equalTo(1))
    },
    
    test("explainRun should detect paused state") {
      val ctx = ExecutionContext(
        query = "Test",
        sessionId = "pause-test",
        state = WorkflowState(status = "paused")
      )
      
      val explanation = explainRun(ctx)
      assert(explanation.paused)(Assertion.equalTo(true))
    },
    
    test("explainRun should aggregate context injections") {
      val ctx = ExecutionContext(
        query = "Test",
        sessionId = "ctx-test",
        trace = List(
          TraceEvent(EventType.ContextInject, "ctx-test", "a1", "Agent", payload = Map(
            "context_names" -> List("ctx1", "ctx2"),
            "count" -> 2,
            "selected_contexts" -> List(
              Map("name" -> "ctx1", "score" -> 0.9, "source" -> "attached", "hops" -> 0),
              Map("name" -> "ctx2", "score" -> 0.8, "source" -> "semantic", "hops" -> 1)
            )
          ))
        )
      )
      
      val explanation = explainRun(ctx)
      assert(explanation.summary.contextInjectionCount)(Assertion.equalTo(1))
    },
    
    test("RunExplanation should have correct structure") {
      val explanation = RunExplanation(
        sessionId = "s1",
        query = "q1",
        hopCount = 2,
        paused = false,
        hops = List(RunHopExplanation("n1", "Node1", Some(1), "summary1")),
        routing = List(RunRoutingExplanation("n1", "Node1", Some("intent1"), Some("next1"), Some(0.9), "llm")),
        toolCalls = List(RunToolCallExplanation("n1", "tool1", "ref1", Map("key" -> "value"))),
        pauses = List(RunPauseExplanation("n1", "PauseNode", None, "waiting", Map())),
        summary = RunSummary(1, 1, 1, 1)
      )
      
      assert(explanation.sessionId)(Assertion.equalTo("s1")) &&
      assert(explanation.hops.length)(Assertion.equalTo(1)) &&
      assert(explanation.routing.length)(Assertion.equalTo(1)) &&
      assert(explanation.toolCalls.length)(Assertion.equalTo(1)) &&
      assert(explanation.summary.toolCallCount)(Assertion.equalTo(1))
    },
    
    test("TraceView should initialize with empty lists") {
      val view = TraceView()
      assert(view.hops)(Assertion.equalTo(Nil)) &&
      assert(view.agentStarts)(Assertion.equalTo(Nil)) &&
      assert(view.agentEnds)(Assertion.equalTo(Nil)) &&
      assert(view.toolCalls)(Assertion.equalTo(Nil))
    }
  )
}