package loki.engine.retrieval

import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.store.*
import loki.engine.backend.*
import scala.collection.mutable

class Embedder(model: String = "all-MiniLM-L6-v2") {
  /** Reflection-loaded sentence-transformers client, memoized once; falls back
    * to the zero-vector DummyEmbedder when the Python bridge is unavailable. */
  private lazy val client: Any =
    try {
      val SentenceTransformer = Class.forName("sentence_transformers.SentenceTransformer")
      val getInstance = SentenceTransformer.getMethod("load", classOf[String])
      getInstance.invoke(null, model)
    } catch {
      case e: Exception =>
        println(s"Warning: Could not load sentence-transformers: $e")
        new DummyEmbedder
    }

  def embedText(text: String): Task[List[Double]] = ZIO.attempt {
    val encodeMethod = client.getClass.getMethod("encode", classOf[String], classOf[Boolean])
    val vector = encodeMethod.invoke(client, text, java.lang.Boolean.TRUE)
    vector match {
      case arr: Array[_] => arr.map(_.asInstanceOf[Float].toDouble).toList
      case _ => List.fill(384)(0.0)
    }
  }

  def embedNode(store: GraphStore, node: AnyNode): Task[AnyNode] = {
    val text = nodeText(node)
    for {
      vec <- embedText(text)
      updated <- node match {
        case n: AgentNode => store.upsertNode(n.copy(embedding = Some(vec))).as(n)
        case n: ContextNode => store.upsertNode(n.copy(embedding = Some(vec))).as(n)
        case n: ToolNode => store.upsertNode(n.copy(embedding = Some(vec))).as(n)
        case n: PromptNode => store.upsertNode(n.copy(embedding = Some(vec))).as(n)
        case n: SchemaNode => store.upsertNode(n.copy(embedding = Some(vec))).as(n)
        case n: GraphNode => store.upsertNode(n.copy(embedding = Some(vec))).as(n)
        case n: ApprovalNode  => store.upsertNode(n.copy(embedding = Some(vec))).as(n)
        case n: TransformNode => store.upsertNode(n.copy(embedding = Some(vec))).as(n)
        case n: ReasonerNode  => store.upsertNode(n.copy(embedding = Some(vec))).as(n)
        case n: BaseNode      => store.upsertNode(n.copy(embedding = Some(vec))).as(n)
      }
    } yield updated
  }

  def embedAll(store: GraphStore, nodeTypes: Option[List[NodeType]] = None, skipExisting: Boolean = true): Task[Int] = {
    for {
      nodes <- store.listNodes(onlyValid = false)
      filtered = nodes.filter(n => nodeTypes.isEmpty || nodeTypes.get.contains(n.nodeType))
      filtered2 = if (skipExisting) filtered.filter(_.embedding.isEmpty) else filtered
      count <- ZIO.foreach(filtered2)(embedNode(store, _)).as(filtered2.length)
    } yield count
  }

  private def nodeText(node: AnyNode): String = {
    val parts = mutable.ListBuffer[String]()
    if (node.name.nonEmpty) parts += node.name
    if (node.description.nonEmpty) parts += node.description
    node match {
      case c: ContextNode if c.content.nonEmpty => parts += c.content.take(2000)
      case t: ToolNode if t.inputSchema.nonEmpty => parts += t.inputSchema.toString.take(500)
      case _ =>
    }
    if (parts.isEmpty) node.nodeId else parts.mkString(": ")
  }
}

class DummyEmbedder {
  def encode(text: String, normalize: Boolean): Array[Float] = {
    Array.fill(384)(0.0f)
  }
}
