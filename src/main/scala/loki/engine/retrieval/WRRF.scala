package loki.engine.retrieval

import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*

/** Score for a single tool contributing to an agent hit. */
case class ToolScore(
  toolId: String,
  toolName: String,
  score: Double,
  rank: Int
)

/** Result of semantic agent search using Weighted Reciprocal Rank Fusion. */
case class RetrievalResult(
  agent: AgentNode,
  score: Double,
  viaTools: List[ToolScore] = Nil,
  toolScores: Map[String, Double] = Map.empty
)

/**
 * Weighted Reciprocal Rank Fusion (wRRF) for finding the best agent given a
 * query embedding.  Two separate searches are fused:
 *
 *  - Tool-based path: find matching ToolNodes, then walk upstream HAS_TOOL
 *    edges to find their parent AgentNodes.
 *  - Agent-based path: find matching AgentNodes directly.
 *
 * Scores from both paths are merged with:
 *   finalScore = w_tool * (1 / (k + rank_tools)) + w_agent * (1 / (k + rank_agents))
 *
 * Optional query tag bonuses are added on top.
 */
object WRRF {

  def semanticSearch(
    store: GraphStore,
    queryEmbedding: List[Double],
    topK: Int = 5,
    topKTools: Int = 20,
    topKAgents: Int = 10,
    wTool: Double = 0.7,
    wAgent: Double = 0.3,
    k: Int = 60,
    queryTags: List[String] = Nil,
    tagWeight: Double = 0.1
  ): URIO[Any, List[RetrievalResult]] =
    for {
      // --- Tool-based path ---
      toolHits <- store.vectorSearch(queryEmbedding, Some(List(NodeType.Tool)), topKTools)
      toolRankMap = rankMap(toolHits)
      // Walk upstream: for each tool hit, find parent agents
      toolAgentResolved <- agentsViaTools(store, toolHits, toolRankMap, topKTools)

      // --- Agent-based path ---
      agentHits <- store.vectorSearch(queryEmbedding, Some(List(NodeType.Agent)), topKAgents)
      agentRankMap = rankMap(agentHits)

      // --- Fuse scores using wRRF ---
      allAgentIds = (toolAgentResolved.map(_._1.nodeId) ++ agentRankMap.keys).distinct
      results <- ZIO.foreach(allAgentIds) { agentId =>
        fusedResult(store, agentId, toolAgentResolved, agentHits, agentRankMap,
          topKTools, wTool, wAgent, k, queryTags, tagWeight)
      }
    } yield results.flatten.sortBy(-_.score).take(topK)

  private def rankMap(hits: List[(AnyNode, Double)]): Map[String, (Double, Int)] =
    hits.zipWithIndex.map { case ((n, score), rank) => n.nodeId -> (score, rank + 1) }.toMap

  /** Agents that own at least one tool the query matched, with those tool hits. */
  private def agentsViaTools(
    store: GraphStore,
    toolHits: List[(AnyNode, Double)],
    toolRankMap: Map[String, (Double, Int)],
    topKTools: Int
  ): URIO[Any, List[(AgentNode, List[ToolScore])]] =
    for {
      candidates <- ZIO.foldLeft(toolHits)(Map.empty[String, List[ToolScore]]) {
        case (acc, (toolNode, toolScore)) =>
          store.getEdges(toolNode.nodeId, Some(EdgeType.HAS_TOOL), direction = "in").map { parentEdges =>
            val ts = ToolScore(toolNode.nodeId, toolNode.name, toolScore,
              toolRankMap.get(toolNode.nodeId).map(_._2).getOrElse(topKTools))
            parentEdges.foldLeft(acc)((a, edge) => a.updated(edge.srcId, ts :: a.getOrElse(edge.srcId, Nil)))
          }
      }
      resolved <- ZIO.foreach(candidates.toList) { case (agentId, toolScores) =>
        store.getNode(agentId).map {
          case Some(a: AgentNode) if a.isValid => Some((a, toolScores))
          case _ => None
        }
      }.map(_.flatten)
    } yield resolved

  /** wRRF score for one agent over both paths, plus the query-tag bonus. */
  private def fusedResult(
    store: GraphStore,
    agentId: String,
    toolAgentResolved: List[(AgentNode, List[ToolScore])],
    agentHits: List[(AnyNode, Double)],
    agentRankMap: Map[String, (Double, Int)],
    topKTools: Int,
    wTool: Double,
    wAgent: Double,
    k: Int,
    queryTags: List[String],
    tagWeight: Double
  ): URIO[Any, Option[RetrievalResult]] = {
    val toolEntry = toolAgentResolved.find(_._1.nodeId == agentId)
    val agentEntry = agentRankMap.get(agentId)

    val toolRRF = toolEntry.map { case (_, ts) =>
      val bestRank = ts.map(_.rank).minOption.getOrElse(topKTools)
      wTool * (1.0 / (k + bestRank))
    }.getOrElse(0.0)

    val agentRRF = agentEntry.map { case (_, rank) =>
      wAgent * (1.0 / (k + rank))
    }.getOrElse(0.0)

    // Tag bonus
    val tagBonus = toolEntry.map { case (agent, _) =>
      val agentTags = agent.attributes.get("tags").map(_.asInstanceOf[List[String]]).getOrElse(Nil)
      val matchCount = queryTags.count(agentTags.contains)
      if (matchCount > 0) tagWeight * matchCount.toDouble / queryTags.length.max(1) else 0.0
    }.getOrElse(0.0)

    val finalScore = toolRRF + agentRRF + tagBonus

    toolEntry match {
      case Some((agent, ts)) =>
        ZIO.succeed(Some(RetrievalResult(
          agent = agent,
          score = finalScore,
          viaTools = ts,
          toolScores = ts.map(t => t.toolId -> t.score).toMap
        )))
      case None =>
        agentHits.find(_._1.nodeId == agentId) match {
          case Some((a: AgentNode, _)) =>
            ZIO.succeed(Some(RetrievalResult(agent = a, score = finalScore)))
          case _ =>
            store.getNode(agentId).map {
              case Some(a: AgentNode) if a.isValid =>
                Some(RetrievalResult(agent = a, score = finalScore))
              case _ => None
            }
        }
    }
  }
}
