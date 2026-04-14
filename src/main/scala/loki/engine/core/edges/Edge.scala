package loki.engine.core.edges

import java.time.Instant
import java.util.UUID
import loki.engine.core.nodes.EdgeType

case class Edge(
  edgeId: String = UUID.randomUUID().toString,
  edgeType: EdgeType = EdgeType.HAS_CONTEXT,
  srcId: String = "",
  dstId: String = "",
  weight: Double = 1.0,
  description: String = "",
  // Domain relation label for RELATES edges (e.g. "targets", "presented_by").
  // Domain-agnostic: loki never interprets this value — it exists so consumers
  // can model a typed knowledge graph without widening EdgeType.
  relation: Option[String] = None,
  attributes: Map[String, Any] = Map.empty,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  graphRevisionId: String = "",
  requiredRoles: List[String] = Nil
) {
  def isValid: Boolean = {
    val now = Instant.now()
    validAt.isBefore(now) && (invalidAt.isEmpty || invalidAt.get.isAfter(now))
  }

  def expire(): Edge = copy(invalidAt = Some(Instant.now()))
}

object Edge {
  def hasTool(srcId: String, dstId: String, weight: Double = 1.0, attrs: Map[String, Any] = Map.empty) =
    Edge(edgeType = EdgeType.HAS_TOOL, srcId = srcId, dstId = dstId, weight = weight, attributes = attrs)

  def hasContext(srcId: String, dstId: String, weight: Double = 1.0, attrs: Map[String, Any] = Map.empty) =
    Edge(edgeType = EdgeType.HAS_CONTEXT, srcId = srcId, dstId = dstId, weight = weight, attributes = attrs)

  def hasPrompt(srcId: String, dstId: String, weight: Double = 1.0, attrs: Map[String, Any] = Map.empty) =
    Edge(edgeType = EdgeType.HAS_PROMPT, srcId = srcId, dstId = dstId, weight = weight, attributes = attrs)

  def delegatesTo(srcId: String, dstId: String, weight: Double = 1.0, attrs: Map[String, Any] = Map.empty) =
    Edge(edgeType = EdgeType.DELEGATES_TO, srcId = srcId, dstId = dstId, weight = weight, attributes = attrs)

  def produces(srcId: String, dstId: String, weight: Double = 1.0, attrs: Map[String, Any] = Map.empty) =
    Edge(edgeType = EdgeType.PRODUCES, srcId = srcId, dstId = dstId, weight = weight, attributes = attrs)

  def similarTo(srcId: String, dstId: String, weight: Double = 1.0, attrs: Map[String, Any] = Map.empty) =
    Edge(edgeType = EdgeType.SIMILAR_TO, srcId = srcId, dstId = dstId, weight = weight, attributes = attrs)

  /** Create a RELATES edge carrying a consumer-defined `relation` label.
   *
   *  Use this to model domain knowledge graphs (e.g. a form field that a rule
   *  `targets`, or a widget that `presents` a field) without adding
   *  domain-specific entries to EdgeType.
   */
  def relates(srcId: String, dstId: String, relation: String, weight: Double = 1.0, attrs: Map[String, Any] = Map.empty) =
    Edge(edgeType = EdgeType.RELATES, srcId = srcId, dstId = dstId, relation = Some(relation), weight = weight, attributes = attrs)
}
