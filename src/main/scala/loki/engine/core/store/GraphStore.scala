package loki.engine.core.store

import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import java.util.UUID
import scala.collection.mutable

trait GraphStore {
  def upsertNode(node: Node): UIO[Unit]
  def upsertEdge(edge: Edge): UIO[Unit]
  def getNode(nodeId: String): UIO[Option[AnyNode]]
  def getEdge(edgeId: String): UIO[Option[Edge]]
  def getEdges(nodeId: String, edgeType: Option[EdgeType] = None, direction: String = "out", onlyValid: Boolean = true, relation: Option[String] = None): UIO[List[Edge]]
  def neighbors(nodeId: String, edgeType: Option[EdgeType] = None, depth: Int = 1, onlyValid: Boolean = true): UIO[List[AnyNode]]
  def vectorSearch(embedding: List[Double], nodeTypes: Option[List[NodeType]] = None, topK: Int = 10, onlyValid: Boolean = true): UIO[List[(AnyNode, Double)]]
  def deleteNode(nodeId: String): UIO[Unit]
  def deleteEdge(edgeId: String): UIO[Unit]
  def listNodes(nodeType: Option[NodeType] = None, groupId: Option[String] = None, onlyValid: Boolean = true, domainType: Option[String] = None): UIO[List[AnyNode]]
  def listEdges(edgeType: Option[EdgeType] = None, groupId: Option[String] = None, onlyValid: Boolean = true, relation: Option[String] = None): UIO[List[Edge]]
  def expireNode(nodeId: String): UIO[Unit]
  def expireEdge(edgeId: String): UIO[Unit]
  def attachContext(agentId: String, ctxId: String, weight: Option[Double] = None, attrs: Map[String, Any] = Map.empty): UIO[Edge]
  def attachTool(agentId: String, toolId: String, weight: Option[Double] = None, attrs: Map[String, Any] = Map.empty): UIO[Edge]
}

class InMemoryGraphStore(private val nodes: mutable.Map[String, AnyNode] = mutable.Map.empty,
                         private val edges: mutable.Map[String, Edge] = mutable.Map.empty) extends GraphStore {

  private def cosine(a: List[Double], b: List[Double]): Double =
    if (a.length != b.length) 0.0
    else normalize(a).zip(normalize(b)).map { case (ai, bi) => ai * bi }.sum

  private def normalize(vec: List[Double]): List[Double] = {
    val norm = math.sqrt(vec.map(x => x * x).sum)
    if (norm == 0) vec else vec.map(x => x / norm)
  }

  def upsertNode(node: Node): UIO[Unit] = ZIO.succeed {
    nodes.put(node.nodeId, node.asInstanceOf[AnyNode])
  }

  def upsertEdge(edge: Edge): UIO[Unit] = ZIO.succeed {
    edges.put(edge.edgeId, edge)
  }

  def getNode(nodeId: String): UIO[Option[AnyNode]] = ZIO.succeed {
    nodes.get(nodeId)
  }

  def getEdge(edgeId: String): UIO[Option[Edge]] = ZIO.succeed {
    edges.get(edgeId)
  }

  def getEdges(nodeId: String, edgeType: Option[EdgeType] = None, direction: String = "out", onlyValid: Boolean = true, relation: Option[String] = None): UIO[List[Edge]] =
    ZIO.succeed {
      val raw = direction match {
        case "out" => edges.values.filter(e => e.srcId == nodeId).toList
        case "in" => edges.values.filter(e => e.dstId == nodeId).toList
        case "both" => edges.values.filter(e => e.srcId == nodeId || e.dstId == nodeId).toList
        case _ => Nil
      }
      raw.filter { edge =>
        val validFilter = !onlyValid || edge.isValid
        val typeFilter = edgeType.isEmpty || edge.edgeType == edgeType.get
        val relationFilter = relation.isEmpty || edge.relation == relation
        validFilter && typeFilter && relationFilter
      }
    }

  def neighbors(nodeId: String, edgeType: Option[EdgeType] = None, depth: Int = 1, onlyValid: Boolean = true): UIO[List[AnyNode]] = {
    def expand(visited: Set[String], frontier: Set[String], remaining: Int): UIO[(Set[String], Set[String])] =
      if (remaining <= 0) ZIO.succeed((visited, frontier))
      else {
        val targets = frontier.filterNot(visited).toList
        ZIO.foreach(targets) { nid =>
          getEdges(nid, edgeType, "both", onlyValid).map { edges =>
            edges
              .map(e => if (e.srcId == nid) e.dstId else e.srcId)
              .filterNot(nb => nb == nid || visited.contains(nb))
          }
        }.flatMap { expansions =>
          expand(visited ++ frontier, expansions.flatten.toSet, remaining - 1)
        }
      }

    expand(Set.empty, Set(nodeId), depth).map { case (_, frontier) =>
      frontier.flatMap(nodes.get).filter(n => !onlyValid || n.isValid).toList
    }
  }

  def vectorSearch(embedding: List[Double], nodeTypes: Option[List[NodeType]] = None, topK: Int = 10, onlyValid: Boolean = true): UIO[List[(AnyNode, Double)]] =
    ZIO.succeed {
      val q = normalize(embedding)
      val results = nodes.values
        .filter(n => !onlyValid || n.isValid)
        .filter(n => nodeTypes.isEmpty || nodeTypes.get.contains(n.nodeType))
        .filter(n => n.embedding.isDefined)
        .map(n => (n, cosine(q, n.embedding.get)))
        .toList
        .sortBy(-_._2)
        .take(topK)
      results
    }

  def deleteNode(nodeId: String): UIO[Unit] = ZIO.succeed {
    nodes.remove(nodeId)
    edges.values.foreach { edge =>
      if (edge.srcId == nodeId || edge.dstId == nodeId) {
        edges.remove(edge.edgeId)
      }
    }
  }

  def deleteEdge(edgeId: String): UIO[Unit] = ZIO.succeed {
    edges.remove(edgeId)
  }

  def listNodes(nodeType: Option[NodeType] = None, groupId: Option[String] = None, onlyValid: Boolean = true, domainType: Option[String] = None): UIO[List[AnyNode]] =
    ZIO.succeed {
      nodes.values
        .filter(n => !onlyValid || n.isValid)
        .filter(n => nodeType.isEmpty || n.nodeType == nodeType.get)
        .filter(n => groupId.isEmpty || n.groupId == groupId.get)
        .filter(n => domainType.isEmpty || n.domainType == domainType)
        .toList
    }

  def listEdges(edgeType: Option[EdgeType] = None, groupId: Option[String] = None, onlyValid: Boolean = true, relation: Option[String] = None): UIO[List[Edge]] =
    ZIO.succeed {
      edges.values
        .filter(e => !onlyValid || e.isValid)
        .filter(e => edgeType.isEmpty || e.edgeType == edgeType.get)
        .filter(e => groupId.isEmpty || e.groupId == groupId.get)
        .filter(e => relation.isEmpty || e.relation == relation)
        .toList
    }

  def expireNode(nodeId: String): UIO[Unit] = for {
    nodeOpt <- getNode(nodeId)
    _ <- nodeOpt match {
      case Some(node) => upsertNode(node.expire())
      case None => ZIO.unit
    }
  } yield ()

  def expireEdge(edgeId: String): UIO[Unit] = for {
    edgeOpt <- getEdge(edgeId)
    _ <- edgeOpt match {
      case Some(edge) => upsertEdge(edge.expire())
      case None => ZIO.unit
    }
  } yield ()

  def attachContext(agentId: String, ctxId: String, weight: Option[Double] = None, attrs: Map[String, Any] = Map.empty): UIO[Edge] = for {
    agentOpt <- getNode(agentId)
    ctxOpt <- getNode(ctxId)
    w <- ZIO.succeed {
      (weight, agentOpt, ctxOpt) match {
        case (Some(w), _, _) => w
        case (None, Some(a: AgentNode), Some(c: ContextNode)) if a.embedding.isDefined && c.embedding.isDefined =>
          cosine(a.embedding.get, c.embedding.get)
        case _ => 1.0
      }
    }
    edge = Edge.hasContext(agentId, ctxId, w, attrs)
    _ <- upsertEdge(edge)
  } yield edge

  def attachTool(agentId: String, toolId: String, weight: Option[Double] = None, attrs: Map[String, Any] = Map.empty): UIO[Edge] = for {
    agentOpt <- getNode(agentId)
    toolOpt <- getNode(toolId)
    w <- ZIO.succeed {
      (weight, agentOpt, toolOpt) match {
        case (Some(w), _, _) => w
        case (None, Some(a: AgentNode), Some(t: ToolNode)) if a.embedding.isDefined && t.embedding.isDefined =>
          cosine(a.embedding.get, t.embedding.get)
        case _ => 1.0
      }
    }
    edge = Edge.hasTool(agentId, toolId, w, attrs)
    _ <- upsertEdge(edge)
  } yield edge
}

object GraphStore {
  def make: UIO[InMemoryGraphStore] = ZIO.succeed(new InMemoryGraphStore())
}
