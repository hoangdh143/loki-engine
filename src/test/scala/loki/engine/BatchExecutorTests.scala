package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.core.executor.*
import loki.engine.backend.*

object BatchExecutorTests {

  class MockLLMBackend extends LLMBackend {
    def chat(
      model: String,
      system: String,
      messages: List[Map[String, Any]],
      tools: List[Map[String, Any]],
      maxTokens: Int = 8096
    ): Task[LLMResponse] = ZIO.succeed {
      LLMResponse(text = "Batch response", toolCalls = Nil, stopReason = "end_turn")
    }
    
    def extendMessages(
      messages: List[Map[String, Any]],
      response: LLMResponse,
      toolResults: List[ToolResult]
    ): List[Map[String, Any]] = messages
  }

  def spec = suite("BatchExecutorTests")(
    test("BatchRun should calculate progress correctly") {
      val run = BatchRun(
        runId = "test-run",
        agentId = "agent1",
        total = 10,
        results = Map(
          "0" -> BatchItemResult("0", BatchStatus.Completed),
          "1" -> BatchItemResult("1", BatchStatus.Completed),
          "2" -> BatchItemResult("2", BatchStatus.Failed),
          "3" -> BatchItemResult("3", BatchStatus.Running),
          "4" -> BatchItemResult("4", BatchStatus.Pending)
        )
      )
      
      assert(run.completed)(Assertion.equalTo(2)) &&
      assert(run.failed)(Assertion.equalTo(1)) &&
      assert(run.running)(Assertion.equalTo(1)) &&
      assert(run.pending)(Assertion.equalTo(1)) &&
      assert(run.progress)(Assertion.equalTo(0.3))
    },
    
    test("BatchRun should report completed status when all done and no failures") {
      val run = BatchRun(
        runId = "test",
        agentId = "a1",
        total = 3,
        results = Map(
          "0" -> BatchItemResult("0", BatchStatus.Completed),
          "1" -> BatchItemResult("1", BatchStatus.Completed),
          "2" -> BatchItemResult("2", BatchStatus.Completed)
        ),
        endedAt = Some(java.time.Instant.now())
      )
      
      assert(run.status)(Assertion.equalTo(BatchStatus.Completed))
    },
    
    test("BatchRun should report failed status when all failed") {
      val run = BatchRun(
        runId = "test",
        agentId = "a1",
        total = 2,
        results = Map(
          "0" -> BatchItemResult("0", BatchStatus.Failed),
          "1" -> BatchItemResult("1", BatchStatus.Failed)
        ),
        endedAt = Some(java.time.Instant.now())
      )
      
      assert(run.status)(Assertion.equalTo(BatchStatus.Failed))
    },
    
    test("BatchRun should report partial status when mixed") {
      val run = BatchRun(
        runId = "test",
        agentId = "a1",
        total = 3,
        results = Map(
          "0" -> BatchItemResult("0", BatchStatus.Completed),
          "1" -> BatchItemResult("1", BatchStatus.Failed),
          "2" -> BatchItemResult("2", BatchStatus.Completed)
        ),
        endedAt = Some(java.time.Instant.now())
      )
      
      assert(run.status)(Assertion.equalTo(BatchStatus.Partial))
    },
    
    test("BatchRun should report running when not ended") {
      val run = BatchRun(
        runId = "test",
        agentId = "a1",
        total = 3,
        results = Map(
          "0" -> BatchItemResult("0", BatchStatus.Completed),
          "1" -> BatchItemResult("1", BatchStatus.Running)
        )
      )
      
      assert(run.status)(Assertion.equalTo(BatchStatus.Running))
    },
    
    test("BatchItemResult should calculate duration") {
      val start = java.time.Instant.now().minusSeconds(10)
      val end = java.time.Instant.now()
      val result = BatchItemResult(
        itemId = "1",
        status = BatchStatus.Completed,
        startedAt = Some(start),
        endedAt = Some(end)
      )
      
      assert(result.durationSeconds.getOrElse(0.0) >= 10.0)(Assertion.equalTo(true))
    },
    
    test("BatchRun successfulOutputs should return in order") {
      val run = BatchRun(
        runId = "test",
        agentId = "a1",
        total = 3,
        results = Map(
          "0" -> BatchItemResult("0", BatchStatus.Completed, output = "out0"),
          "1" -> BatchItemResult("1", BatchStatus.Failed),
          "2" -> BatchItemResult("2", BatchStatus.Completed, output = "out2")
        )
      )
      
      val outputs = run.successfulOutputs
      assert(outputs.length)(Assertion.equalTo(2)) &&
      assert(outputs.head)(Assertion.equalTo("out0"))
    },
    
    test("BatchRun with zero total should have progress 1.0") {
      val run = BatchRun(
        runId = "test",
        agentId = "a1",
        total = 0,
        results = Map.empty
      )
      
      assert(run.progress)(Assertion.equalTo(1.0))
    }
  )
}