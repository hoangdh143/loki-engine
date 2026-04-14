package loki.engine

import zio.test.{test, assert, Assertion, suite}
import loki.engine.core.executor.GraphExecutor

object HistoryCompactionTests {
  private val Big = "X" * 5000

  private def anthropicHistory(pairs: Int): List[Map[String, Any]] = {
    val head = List(
      Map[String, Any]("role" -> "user", "content" -> s"TASK $Big"),
      Map[String, Any]("role" -> "assistant", "content" -> s"setup $Big")
    )
    head ++ (0 until pairs).toList.flatMap { i =>
      List(
        Map[String, Any]("role" -> "assistant", "content" -> List(
          Map[String, Any]("type" -> "text", "text" -> s"thinking $Big"),
          Map[String, Any]("type" -> "tool_use", "id" -> s"t$i", "name" -> "q", "input" -> Map.empty)
        )),
        Map[String, Any]("role" -> "user", "content" -> List(
          Map[String, Any]("type" -> "tool_result", "tool_use_id" -> s"t$i", "content" -> s"RESULT $Big")
        ))
      )
    }
  }

  def spec = suite("HistoryCompactionTests")(
    test("shrinks middle while keeping head and recent messages") {
      val messages = anthropicHistory(8)
      val compacted = GraphExecutor.compactHistory(messages)
      assert(compacted.mkString.length < messages.mkString.length)(Assertion.isTrue) &&
      assert(compacted.head("content"))(Assertion.equalTo(s"TASK $Big")) &&
      assert(compacted.last.toString.contains(s"RESULT $Big"))(Assertion.isTrue) &&
      assert(compacted.toString.contains(GraphExecutor.HistoryElideMark))(Assertion.isTrue)
    },

    test("preserves every Anthropic tool-use block") {
      val compacted = GraphExecutor.compactHistory(anthropicHistory(8))
      val toolUses = compacted.flatMap(_.get("content").toList).collect {
        case blocks: List[?] => blocks.collect {
          case block: Map[?, ?] if block.asInstanceOf[Map[String, Any]].get("type").contains("tool_use") => block
        }
      }.flatten
      assert(toolUses.length)(Assertion.equalTo(8))
    },

    test("preserves OpenAI tool calls and compacts idempotently") {
      val messages = List(
        Map[String, Any]("role" -> "user", "content" -> "task"),
        Map[String, Any]("role" -> "user", "content" -> "setup")
      ) ++ (0 until 8).toList.flatMap { i =>
        List(
          Map[String, Any]("role" -> "assistant", "content" -> s"reason $Big", "tool_calls" -> List(
            Map[String, Any]("id" -> s"c$i", "type" -> "function")
          )),
          Map[String, Any]("role" -> "tool", "tool_call_id" -> s"c$i", "content" -> s"RES $Big")
        )
      }
      val once = GraphExecutor.compactHistory(messages)
      val twice = GraphExecutor.compactHistory(once)
      val toolCalls = once.map(_.get("tool_calls").collect { case calls: List[?] => calls.length }.getOrElse(0)).sum
      assert(toolCalls)(Assertion.equalTo(8)) &&
      assert(twice)(Assertion.equalTo(once))
    },

    test("leaves short history and short text untouched") {
      val messages = anthropicHistory(1)
      assert(GraphExecutor.compactHistory(messages))(Assertion.equalTo(messages)) &&
      assert(GraphExecutor.clipForHistory("short text"))(Assertion.equalTo("short text"))
    }
  )
}
