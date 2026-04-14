package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.core.executor.*
import loki.engine.backend.*

/** The ways a local model actually behaves.
  *
  * Each case here was measured against a live LM Studio trace, not inferred:
  * a tool call the server left as text, arguments it half-parsed into markup, an
  * announce-and-stop final turn, and an agent re-run on work that already exists. */
object LocalModelToleranceTests {

  /** Replays a fixed script of responses and records what it was asked. */
  class ScriptedBackend(responses: List[LLMResponse]) extends LLMBackend {
    private val index = new java.util.concurrent.atomic.AtomicInteger(0)
    val calls = new java.util.concurrent.atomic.AtomicInteger(0)
    val lastMessages = new java.util.concurrent.atomic.AtomicReference[List[Map[String, Any]]](Nil)

    def chat(
      model: String,
      system: String,
      messages: List[Map[String, Any]],
      tools: List[Map[String, Any]],
      maxTokens: Int = 8096
    ): Task[LLMResponse] = ZIO.succeed {
      calls.incrementAndGet()
      lastMessages.set(messages)
      val response = responses(math.min(index.get, responses.length - 1))
      index.incrementAndGet()
      response
    }

    def extendMessages(
      messages: List[Map[String, Any]],
      response: LLMResponse,
      toolResults: List[ToolResult]
    ): List[Map[String, Any]] =
      messages :+ Map("role" -> "assistant", "content" -> response.text)
  }

  private def userContents(messages: List[Map[String, Any]]): List[String] =
    messages.filter(_.get("role").contains("user")).flatMap(_.get("content").map(_.toString))

  /** A file that exists while the effect `body` builds runs, and is removed after.
    *
    * The deletion has to be part of the effect: a plain try/finally would delete the
    * file while `body` has only RETURNED a ZIO, before the executor ever looks. */
  private def withTempFile[R, E, A](body: java.nio.file.Path => ZIO[R, E, A]): ZIO[R, E, A] =
    ZIO.acquireReleaseWith(
      ZIO.succeed(java.nio.file.Files.createTempFile("loki-completion-", ".txt"))
    )(path => ZIO.succeed(java.nio.file.Files.deleteIfExists(path)))(body)

  def spec = suite("LocalModelToleranceTests")(

    // --- Recovering tool calls the API layer left as text --------------------

    test("Hermes-style tool call written as text is recovered") {
      val text = """Sure. <tool_call>{"name": "search", "arguments": {"query": "hats"}}</tool_call>"""
      val calls = ToolCallRecovery.parseTextToolCalls(text)
      assert(ToolCallRecovery.looksLikeUnparsedToolCall(text))(Assertion.isTrue) &&
      assert(calls.map(_.name))(Assertion.equalTo(List("search"))) &&
      assert(calls.head.input)(Assertion.equalTo(Map[String, Any]("query" -> "hats")))
    },

    test("Hermes arguments sent as a JSON string are decoded") {
      val text = """<tool_call>{"name": "search", "arguments": "{\"query\": \"hats\"}"}</tool_call>"""
      val calls = ToolCallRecovery.parseTextToolCalls(text)
      assert(calls.head.input)(Assertion.equalTo(Map[String, Any]("query" -> "hats")))
    },

    test("Llama-style function markup is recovered with its parameters") {
      val text = "<function=write_file><parameter=path>\nout.md\n</parameter><parameter=body>hi</parameter></function>"
      val calls = ToolCallRecovery.parseTextToolCalls(text)
      assert(calls.map(_.name))(Assertion.equalTo(List("write_file"))) &&
      assert(calls.head.input)(Assertion.equalTo(Map[String, Any]("path" -> "out.md", "body" -> "hi")))
    },

    test("the Hermes/Llama hybrid is recovered once, not twice") {
      val text = "<tool_call><function=search><parameter=query>hats</parameter></function></tool_call>"
      val calls = ToolCallRecovery.parseTextToolCalls(text)
      assert(calls.map(_.name))(Assertion.equalTo(List("search"))) &&
      assert(calls.head.input)(Assertion.equalTo(Map[String, Any]("query" -> "hats")))
    },

    test("two function blocks do not cross-assign their parameters") {
      val text = "<function=a><parameter=x>1</parameter></function><function=b><parameter=y>2</parameter></function>"
      val calls = ToolCallRecovery.parseTextToolCalls(text)
      assert(calls.map(_.name))(Assertion.equalTo(List("a", "b"))) &&
      assert(calls.head.input)(Assertion.equalTo(Map[String, Any]("x" -> "1"))) &&
      assert(calls(1).input)(Assertion.equalTo(Map[String, Any]("y" -> "2")))
    },

    test("the markup is found in the reasoning channel too") {
      // A reasoning model routinely leaves `content` blank and puts the call it meant
      // to make in `reasoning_content`; checking only `content` misses exactly that.
      val reasoning = """<tool_call>{"name": "search", "arguments": {}}</tool_call>"""
      assert(ToolCallRecovery.looksLikeUnparsedToolCall("\n\n", reasoning))(Assertion.isTrue) &&
      assert(ToolCallRecovery.parseTextToolCalls("\n\n", reasoning).map(_.name))(Assertion.equalTo(List("search")))
    },

    test("prose that merely mentions tools is not treated as a call") {
      val text = "I could call the search tool, or use a function to do it. Here is the answer: 42."
      assert(ToolCallRecovery.looksLikeUnparsedToolCall(text))(Assertion.isFalse)
    },

    test("unparseable markup is detected but yields no call") {
      val text = "<tool_call>{\"name\": "          // cut off mid-write
      assert(ToolCallRecovery.looksLikeUnparsedToolCall(text))(Assertion.isTrue) &&
      assert(ToolCallRecovery.parseTextToolCalls(text))(Assertion.isEmpty)
    },

    test("arguments the server mangled into markup are detected") {
      val mangled = List(ToolCall("1", "search", Map("tags" -> "<parameter=limit>\n5")))
      val clean   = List(ToolCall("1", "search", Map("tags" -> "new,sale")))
      assert(ToolCallRecovery.hasMangledToolArgs(mangled))(Assertion.isTrue) &&
      assert(ToolCallRecovery.hasMangledToolArgs(clean))(Assertion.isFalse)
    },

    // --- The backend's view of all that --------------------------------------

    test("OpenAI backend recovers a text tool call when tools were offered") {
      val backend = new OpenAIBackend(baseUrl = "http://localhost:0")
      val body =
        """{"choices":[{"message":{"content":"<tool_call>{\"name\": \"search\", \"arguments\": {\"q\": \"x\"}}</tool_call>"},"finish_reason":"stop"}]}"""
      val response = backend.parseResponse(body, toolsOffered = true)
      assert(response.stopReason)(Assertion.equalTo("tool_use")) &&
      assert(response.toolCalls.map(_.name))(Assertion.equalTo(List("search")))
    },

    test("unrecoverable markup becomes stop reason unparsed_tool_call") {
      val backend = new OpenAIBackend(baseUrl = "http://localhost:0")
      val body = """{"choices":[{"message":{"content":"<tool_call>{\"name\": "},"finish_reason":"stop"}]}"""
      val response = backend.parseResponse(body, toolsOffered = true)
      assert(response.stopReason)(Assertion.equalTo("unparsed_tool_call")) &&
      assert(response.toolCalls)(Assertion.isEmpty)
    },

    test("with no tools on offer the markup is left as plain text") {
      val backend = new OpenAIBackend(baseUrl = "http://localhost:0")
      val body =
        """{"choices":[{"message":{"content":"<tool_call>{\"name\": \"search\", \"arguments\": {}}</tool_call>"},"finish_reason":"stop"}]}"""
      val response = backend.parseResponse(body)
      assert(response.stopReason)(Assertion.equalTo("end_turn")) &&
      assert(response.toolCalls)(Assertion.isEmpty)
    },

    // --- Turns that are not answers ------------------------------------------

    test("narration lead-ins are told apart from real answers") {
      assert(GraphExecutor.looksLikeNarrationOnly("Now let me resolve all the field contexts:"))(Assertion.isTrue) &&
      assert(GraphExecutor.looksLikeNarrationOnly("   \n\n  "))(Assertion.isTrue) &&
      // A blank turn whose reasoning reached a conclusion is salvaged, not nudged.
      assert(GraphExecutor.looksLikeNarrationOnly("", "the answer is 42"))(Assertion.isFalse) &&
      assert(GraphExecutor.looksLikeNarrationOnly("The answer is 42."))(Assertion.isFalse) &&
      // A real answer that happens to introduce a list has content after the colon.
      assert(GraphExecutor.looksLikeNarrationOnly("Let me explain: the cause is a stale cache."))(Assertion.isFalse) &&
      assert(GraphExecutor.looksLikeNarrationOnly("Here are the results:"))(Assertion.isFalse)
    },

    test("an announce-and-stop turn is nudged, and the real answer is kept") {
      val store = new InMemoryGraphStore()
      val backend = new ScriptedBackend(List(
        LLMResponse("Now let me write the summary:", Nil, "end_turn"),
        LLMResponse("The summary is: all green.", Nil, "end_turn")
      ))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "Writer")

      for {
        _   <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "go")
      } yield {
        val output = ctx.outputs(agent.nodeId).asInstanceOf[Map[String, Any]]
        assert(output("text"))(Assertion.equalTo("The summary is: all green.")) &&
        assert(backend.calls.get())(Assertion.equalTo(2)) &&
        assert(userContents(backend.lastMessages.get()))(Assertion.contains(GraphExecutor.ContinuationNudge))
      }
    },

    test("a model that only ever announces still terminates") {
      val store = new InMemoryGraphStore()
      val backend = new ScriptedBackend(List(LLMResponse("Now let me start:", Nil, "end_turn")))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "Waffler", maxIterations = 20)

      for {
        _   <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "go")
      } yield
        // Bounded by MaxContinuationNudges (2): first turn plus two nudged retries.
        assert(backend.calls.get())(Assertion.equalTo(3)) &&
        assert(ctx.outputs(agent.nodeId).asInstanceOf[Map[String, Any]]("text"))(
          Assertion.equalTo("Now let me start:"))
    },

    test("an unparsed tool call is corrected, bounded, and then accepted as terminal") {
      val store = new InMemoryGraphStore()
      val backend = new ScriptedBackend(List(
        LLMResponse("<tool_call>{\"name\": ", Nil, "unparsed_tool_call")
      ))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "Caller", maxIterations = 20)

      for {
        _   <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "go")
      } yield
        // Bounded by MaxToolCallCorrections (3): first turn plus three corrections.
        assert(backend.calls.get())(Assertion.equalTo(4)) &&
        assert(userContents(backend.lastMessages.get()))(Assertion.contains(GraphExecutor.ToolCallCorrectionPrompt)) &&
        assert(ctx.trace.filter(_.eventType == EventType.Retry).map(_.payload("reason")))(
          Assertion.contains("unparsed_tool_call"))
    },

    // --- completion_paths: both sides of "done" -------------------------------

    test("a terminal turn with a declared path missing is asked to finish") {
      val store = new InMemoryGraphStore()
      val absent = java.nio.file.Paths.get(
        java.lang.System.getProperty("java.io.tmpdir"), s"loki-absent-${java.util.UUID.randomUUID()}.md").toString
      val backend = new ScriptedBackend(List(LLMResponse("All done!", Nil, "end_turn")))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "Producer", maxIterations = 20,
        attributes = Map("completion_paths" -> List(absent)))

      for {
        _   <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "go")
      } yield
        // Bounded by MaxCompletionNudges (2): first turn plus two nudges.
        assert(backend.calls.get())(Assertion.equalTo(3)) &&
        assert(userContents(backend.lastMessages.get()).exists(_.contains(absent)))(Assertion.isTrue) &&
        assert(ctx.trace.filter(_.eventType == EventType.Retry).map(_.payload("reason")))(
          Assertion.contains("missing_completion_paths"))
    },

    test("an agent whose declared paths all exist is skipped without an LLM turn") {
      withTempFile { existing =>
        val store = new InMemoryGraphStore()
        val backend = new ScriptedBackend(List(LLMResponse("should never run", Nil, "end_turn")))
        val executor = new GraphExecutor(store, backend, new AgentComposer(store))
        val agent = AgentNode(name = "AlreadyDone",
          attributes = Map("completion_paths" -> List(existing.toString)))

        for {
          _   <- store.upsertNode(agent)
          ctx <- executor.run(agent.nodeId, "go")
        } yield {
          val output = ctx.outputs(agent.nodeId).asInstanceOf[Map[String, Any]]
          assert(backend.calls.get())(Assertion.equalTo(0)) &&
          assert(output("skipped"))(Assertion.equalTo(true)) &&
          assert(ctx.trace.map(_.eventType))(Assertion.contains(EventType.AgentSkip))
        }
      }
    },

    test("a node declaring no completion paths is untouched") {
      val store = new InMemoryGraphStore()
      val backend = new ScriptedBackend(List(LLMResponse("done", Nil, "end_turn")))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "Plain")

      for {
        _   <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "go")
      } yield
        assert(backend.calls.get())(Assertion.equalTo(1)) &&
        assert(ctx.trace.map(_.eventType))(Assertion.not(Assertion.contains(EventType.AgentSkip)))
    },

    // --- An unknown tool names the belt ---------------------------------------

    test("an unknown tool call is answered with the tools that do exist") {
      val store = new InMemoryGraphStore()
      val backend = new ScriptedBackend(List(
        LLMResponse("", List(ToolCall("c1", "read_file", Map.empty)), "tool_use"),
        LLMResponse("ok", Nil, "end_turn")
      ))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "Leashed")
      val writer = ToolNode(name = "write_file", callableRef = "write_file")
      val reader = ToolNode(name = "read_input", callableRef = "read_input")
      executor.registerTool("write_file", _ => "written")
      executor.registerTool("read_input", _ => "read")

      for {
        _   <- store.upsertNode(agent)
        _   <- store.upsertNode(writer)
        _   <- store.upsertNode(reader)
        _   <- store.upsertEdge(Edge.hasTool(agent.nodeId, writer.nodeId))
        _   <- store.upsertEdge(Edge.hasTool(agent.nodeId, reader.nodeId))
        ctx <- executor.run(agent.nodeId, "go")
      } yield {
        val summaries = ctx.trace
          .filter(e => e.eventType == EventType.ToolResult && e.nodeName == "read_file")
          .map(_.payload("output_summary").toString)
        assert(summaries.exists(_.contains("not found")))(Assertion.isTrue) &&
        assert(summaries.exists(s => s.contains("read_input") && s.contains("write_file")))(Assertion.isTrue)
      }
    },

    test("a node with no tools at all is told to answer directly") {
      val store = new InMemoryGraphStore()
      val backend = new ScriptedBackend(List(
        LLMResponse("", List(ToolCall("c1", "read_file", Map.empty)), "tool_use"),
        LLMResponse("ok", Nil, "end_turn")
      ))
      val executor = new GraphExecutor(store, backend, new AgentComposer(store))
      val agent = AgentNode(name = "Bare")

      for {
        _   <- store.upsertNode(agent)
        ctx <- executor.run(agent.nodeId, "go")
      } yield {
        val summaries = ctx.trace
          .filter(_.eventType == EventType.ToolResult)
          .map(_.payload("output_summary").toString)
        assert(summaries.exists(_.contains("has no tools")))(Assertion.isTrue)
      }
    },

    // --- Schema-directed coercion of tool inputs -------------------------------

    test("a comma-separated string for an array parameter is coerced") {
      val schema = Map[String, Any]("properties" -> Map[String, Any](
        "tags" -> Map[String, Any]("type" -> "array")))
      assert(GraphExecutor.coerceToSchema(Map("tags" -> "new, sale"), schema))(
        Assertion.equalTo(Map[String, Any]("tags" -> List("new", "sale"))))
    },

    test("array items are coerced to the declared item type") {
      val schema = Map[String, Any]("properties" -> Map[String, Any](
        "ids" -> Map[String, Any]("type" -> "array", "items" -> Map[String, Any]("type" -> "integer"))))
      assert(GraphExecutor.coerceToSchema(Map("ids" -> "1,2,3"), schema))(
        Assertion.equalTo(Map[String, Any]("ids" -> List(1, 2, 3))))
    },

    test("stringified scalars are coerced to the declared type") {
      val schema = Map[String, Any]("properties" -> Map[String, Any](
        "limit"   -> Map[String, Any]("type" -> "integer"),
        "ratio"   -> Map[String, Any]("type" -> "number"),
        "dry_run" -> Map[String, Any]("type" -> "boolean")))
      assert(GraphExecutor.coerceToSchema(Map("limit" -> "5", "ratio" -> "0.5", "dry_run" -> "yes"), schema))(
        Assertion.equalTo(Map[String, Any]("limit" -> 5, "ratio" -> 0.5, "dry_run" -> true)))
    },

    test("nothing is invented: wrong-shaped and undeclared values pass through") {
      val schema = Map[String, Any]("properties" -> Map[String, Any](
        "limit" -> Map[String, Any]("type" -> "integer"),
        "tags"  -> Map[String, Any]("type" -> "array")))
      val payload = Map[String, Any](
        "limit"    -> "not-a-number",      // unparseable: left for validation to judge
        "tags"     -> List("already"),     // already the right type
        "whatever" -> "untouched")         // undeclared
      assert(GraphExecutor.coerceToSchema(payload, schema))(Assertion.equalTo(payload)) &&
      assert(GraphExecutor.coerceToSchema(payload, Map.empty))(Assertion.equalTo(payload))
    },

    // --- Runtime knobs are read when used, not when the class loads -------------

    test("a token knob set after the engine loaded is honoured") {
      val property = "loki.token.escalation.cap"
      val previous = Option(java.lang.System.getProperty(property))
      try {
        java.lang.System.setProperty(property, "13200")
        val live = GraphExecutor.TokenEscalationCap
        java.lang.System.clearProperty(property)
        assert(live)(Assertion.equalTo(13200)) &&
        assert(GraphExecutor.TokenEscalationCap)(Assertion.equalTo(32000))
      } finally previous.foreach(java.lang.System.setProperty(property, _))
    },

    test("a malformed knob falls back to the default instead of failing the run") {
      val property = "loki.max.token.escalations"
      val previous = Option(java.lang.System.getProperty(property))
      try {
        java.lang.System.setProperty(property, "lots")
        assert(GraphExecutor.MaxTokenEscalations)(Assertion.equalTo(2))
      } finally {
        java.lang.System.clearProperty(property)
        previous.foreach(java.lang.System.setProperty(property, _))
      }
    }
  )
}
