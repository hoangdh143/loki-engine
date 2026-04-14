package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import loki.engine.core.edges.*
import loki.engine.core.nodes.EdgeType

object EdgeTests {

  def spec = suite("EdgeTests")(
    test("Edge should have correct defaults") {
      val edge = Edge()
      assert(edge.edgeType)(Assertion.equalTo(EdgeType.HAS_CONTEXT)) &&
      assert(edge.weight)(Assertion.equalTo(1.0)) &&
      assert(edge.isValid)(Assertion.equalTo(true))
    },
    
    test("Edge.hasTool factory method") {
      val edge = Edge.hasTool("agent1", "tool1")
      assert(edge.edgeType)(Assertion.equalTo(EdgeType.HAS_TOOL)) &&
      assert(edge.srcId)(Assertion.equalTo("agent1")) &&
      assert(edge.dstId)(Assertion.equalTo("tool1"))
    },
    
    test("Edge.hasContext factory method") {
      val edge = Edge.hasContext("agent1", "ctx1", weight = 0.8)
      assert(edge.edgeType)(Assertion.equalTo(EdgeType.HAS_CONTEXT)) &&
      assert(edge.weight)(Assertion.equalTo(0.8)) &&
      assert(edge.srcId)(Assertion.equalTo("agent1")) &&
      assert(edge.dstId)(Assertion.equalTo("ctx1"))
    },
    
    test("Edge.delegatesTo factory method") {
      val edge = Edge.delegatesTo("agent1", "agent2")
      assert(edge.edgeType)(Assertion.equalTo(EdgeType.DELEGATES_TO)) &&
      assert(edge.srcId)(Assertion.equalTo("agent1")) &&
      assert(edge.dstId)(Assertion.equalTo("agent2"))
    },
    
    test("Edge.produces factory method") {
      val edge = Edge.produces("tool1", "ctx1")
      assert(edge.edgeType)(Assertion.equalTo(EdgeType.PRODUCES)) &&
      assert(edge.srcId)(Assertion.equalTo("tool1")) &&
      assert(edge.dstId)(Assertion.equalTo("ctx1"))
    },
    
    test("Edge.similarTo factory method") {
      val edge = Edge.similarTo("ctx1", "ctx2", weight = 0.95)
      assert(edge.edgeType)(Assertion.equalTo(EdgeType.SIMILAR_TO)) &&
      assert(edge.weight)(Assertion.equalTo(0.95))
    },
    
    test("Edge expire should set invalid_at") {
      val edge = Edge.hasTool("a", "b")
      val expired = edge.expire()
      assert(expired.isValid)(Assertion.equalTo(false))
      assert(expired.invalidAt.isDefined)(Assertion.equalTo(true))
    }
  )
}