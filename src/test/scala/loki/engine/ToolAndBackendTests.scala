package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import zio.*
import loki.engine.backend.*
import loki.engine.tools.*

object ToolAndBackendTests {

  def spec = suite("ToolAndBackendTests")(
    test("ToolCall should hold correct data") {
      val tc = ToolCall("id1", "search", Map("query" -> "test"))
      assert(tc.id)(Assertion.equalTo("id1")) &&
      assert(tc.name)(Assertion.equalTo("search")) &&
      assert(tc.input("query"))(Assertion.equalTo("test"))
    },
    
    test("ToolResult should hold correct data") {
      val tr = ToolResult("call-id", "Result content")
      assert(tr.toolCallId)(Assertion.equalTo("call-id")) &&
      assert(tr.content)(Assertion.equalTo("Result content"))
    },
    
    test("LLMResponse should hold text and tool calls") {
      val response = LLMResponse(
        text = "Hello world",
        toolCalls = List(ToolCall("id1", "tool1", Map())),
        stopReason = "end_turn"
      )
      assert(response.text)(Assertion.equalTo("Hello world")) &&
      assert(response.toolCalls.length)(Assertion.equalTo(1)) &&
      assert(response.stopReason)(Assertion.equalTo("end_turn")) &&
      assert(response.reasoning)(Assertion.equalTo(""))
    },

    test("OpenAI responses preserve reasoning and token truncation") {
      val backend = new OpenAIBackend(baseUrl = "http://localhost:0")
      val response = backend.parseResponse(
        """{"choices":[{"message":{"content":null,"reasoning_content":"thinking"},"finish_reason":"length"}]}"""
      )
      assert(response.text)(Assertion.equalTo("")) &&
      assert(response.reasoning)(Assertion.equalTo("thinking")) &&
      assert(response.stopReason)(Assertion.equalTo("max_tokens"))
    },

    test("OpenAI responses safely parse large reasoning payloads") {
      val backend = new OpenAIBackend(baseUrl = "http://localhost:0")
      val reasoning = "reasoning " * 20000
      val response = backend.parseResponse(
        s"""{"choices":[{"message":{"content":"DONE","reasoning_content":"$reasoning"},"finish_reason":"stop"}]}"""
      )
      assert(response.text)(Assertion.equalTo("DONE")) &&
      assert(response.reasoning.length)(Assertion.equalTo(reasoning.length)) &&
      assert(response.stopReason)(Assertion.equalTo("end_turn"))
    },

    test("Anthropic responses preserve thinking and token truncation") {
      val backend = new AnthropicBackend(apiKey = "test", baseUrl = "http://localhost:0")
      val response = backend.parseResponse(
        """{"content":[{"type":"thinking","thinking":"working"}],"stop_reason":"max_tokens"}"""
      )
      assert(response.text)(Assertion.equalTo("")) &&
      assert(response.reasoning)(Assertion.equalTo("working")) &&
      assert(response.stopReason)(Assertion.equalTo("max_tokens"))
    },
    
    test("EchoTool should return input") {
      val result = EchoTool("test input")
      assert(result)(Assertion.equalTo("test input"))
    },
    
    test("EchoTool.run should handle map input") {
      val result = EchoTool.run(Map("text" -> "hello"))
      assert(result)(Assertion.equalTo("hello"))
    },
    
    test("WebSearchTool should format query") {
      val result = WebSearchTool("scala zio")
      assert(result)(Assertion.equalTo("Search results for: scala zio"))
    },
    
    test("WebSearchTool.run should extract query from map") {
      val result = WebSearchTool.run(Map("query" -> "Scala"))
      assert(result)(Assertion.equalTo("Search results for: Scala"))
    },
    
    test("CodeExecTool should format code input") {
      val result = CodeExecTool("val x = 1")
      assert(result.contains("val x = 1"))(Assertion.equalTo(true))
    },
    
    test("DefaultToolRegistry should register tools") {
      val registry = DefaultTools.registry
      assert(registry.size >= 2)(Assertion.equalTo(true))
    },
    
    test("DefaultToolRegistry should get registered tool") {
      val registry = DefaultTools.registry
      val tool = registry.get("loki.engine.tools.web_search.search")
      assert(tool.isDefined)(Assertion.equalTo(true))
    },
    
    test("DefaultToolRegistry should return false for unregistered") {
      val registry = DefaultTools.registry
      assert(registry.contains("nonexistent.tool"))(Assertion.equalTo(false))
    },
    
    test("DefaultToolRegistry items should be iterable") {
      val registry = DefaultTools.registry
      val items = registry.items.toList
      assert(items.nonEmpty)(Assertion.equalTo(true))
    }
  )
}
