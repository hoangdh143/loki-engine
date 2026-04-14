package loki.engine.core.executor

import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.backend.*
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.immutable.Map as IMap
import scala.jdk.CollectionConverters.*

case class TraceEvent(
  eventType: EventType,
  sessionId: String,
  nodeId: String,
  nodeName: String,
  timestamp: Instant = Instant.now(),
  payload: Map[String, Any] = Map.empty,
  eventId: String = UUID.randomUUID().toString,
  parentEventId: Option[String] = None,
  durationMs: Option[Long] = None
)

case class RoutingDecision(
  agentId: String,
  reason: String,
  confidence: Double
) {
  def lowConfidenceWarning: Option[String] = 
    if (confidence < 0.7) Some(s"Low-confidence routing (${(confidence * 100).toInt}%) — consider specifying the agent explicitly.")
    else None
}

case class AgentResult(
  routedTo: String,
  reason: String,
  confidence: Double,
  contextInjected: List[String],
  result: String,
  lowConfidenceWarning: Option[String] = None
)

case class WorkflowPause(
  reason: String,
  nodeId: String,
  nodeName: String = "",
  token: String = UUID.randomUUID().toString,
  waitingFor: Option[String] = None,
  metadata: Map[String, Any] = Map.empty
)

case class ApprovalTask(
  taskId: String,
  nodeId: String,
  token: String = UUID.randomUUID().toString,
  status: String = "pending",
  assignees: List[String] = Nil,
  assignedTo: Option[String] = None,
  waitingFor: Option[String] = None,
  dueAt: Option[Instant] = None,
  escalationTarget: Option[String] = None,
  createdAt: Instant = Instant.now(),
  resolvedAt: Option[Instant] = None,
  metadata: Map[String, Any] = Map.empty
)

case class WorkflowState(
  data: Map[String, Any] = Map.empty,
  schema: Map[String, Any] = Map.empty,
  status: String = "running",
  graphVersion: String = "v1",
  pendingPause: Option[WorkflowPause] = None,
  inbox: Map[String, ApprovalTask] = Map.empty,
  idempotencyCache: Map[String, Any] = Map.empty,
  metadata: Map[String, Any] = Map.empty
)

case class ResumeReadiness(
  status: String,
  isPaused: Boolean,
  isStale: Boolean,
  lastEventAt: Option[java.time.Instant],
  secondsSinceLastEvent: Option[Double],
  available: List[String],
  unrecoverable: List[String]
) {
  def ok: Boolean = !isStale && unrecoverable.isEmpty
}

case class ExecutionContext(
  query: Any,
  sessionId: String = UUID.randomUUID().toString,
  outputs: Map[String, Any] = Map.empty,
  trace: List[TraceEvent] = Nil,
  maxHops: Int = 20,
  hopCount: Int = 0,
  extraMessages: List[Map[String, Any]] = Nil,
  state: WorkflowState = WorkflowState(),
  allowedTools: Option[Set[String]] = None,
  activeSubgraphs: List[String] = Nil
) {
  def isPaused: Boolean = state.status == "paused"

  def snapshot: Map[String, Any] = Map(
    "query" -> query,
    "session_id" -> sessionId,
    "outputs" -> outputs,
    "trace" -> trace.map { e =>
      Map(
        "event_type" -> e.eventType.value,
        "session_id" -> e.sessionId,
        "node_id" -> e.nodeId,
        "node_name" -> e.nodeName,
        "timestamp" -> e.timestamp.toString,
        "payload" -> e.payload,
        "event_id" -> e.eventId,
        "parent_event_id" -> e.parentEventId,
        "duration_ms" -> e.durationMs
      )
    },
    "max_hops" -> maxHops,
    "hop_count" -> hopCount,
    "extra_messages" -> extraMessages,
    "state" -> Map(
      "data" -> state.data,
      "schema" -> state.schema,
      "status" -> state.status,
      "graph_version" -> state.graphVersion,
      "pending_pause" -> state.pendingPause.map(p => Map(
        "reason" -> p.reason,
        "node_id" -> p.nodeId,
        "node_name" -> p.nodeName,
        "token" -> p.token,
        "waiting_for" -> p.waitingFor,
        "metadata" -> p.metadata
      )),
      "inbox" -> state.inbox.map { case (k, v) => k -> Map(
        "task_id" -> v.taskId,
        "node_id" -> v.nodeId,
        "token" -> v.token,
        "status" -> v.status,
        "assignees" -> v.assignees,
        "assigned_to" -> v.assignedTo,
        "waiting_for" -> v.waitingFor,
        "due_at" -> v.dueAt.map(_.toString),
        "escalation_target" -> v.escalationTarget,
        "created_at" -> v.createdAt.toString,
        "resolved_at" -> v.resolvedAt.map(_.toString),
        "metadata" -> v.metadata
      )},
      "idempotency_cache" -> state.idempotencyCache,
      "metadata" -> state.metadata
    ),
    "allowed_tools" -> allowedTools.map(_.toList.sorted)
  )

  def withTrace(event: TraceEvent): ExecutionContext = copy(trace = event :: trace)
}

object ExecutionContext {
  def fromSnapshot(data: Map[String, Any]): ExecutionContext = {
    val rawState = data.get("state").collect { case m: Map[String, Any] => m }.getOrElse(Map.empty)
    val pendingPause = rawState.get("pending_pause").collect { case m: Map[String, Any] =>
      WorkflowPause(
        reason     = m.getOrElse("reason", "").toString,
        nodeId     = m.getOrElse("node_id", "").toString,
        nodeName   = m.getOrElse("node_name", "").toString,
        token      = m.getOrElse("token", UUID.randomUUID().toString).toString,
        waitingFor = m.get("waiting_for").map(_.toString),
        metadata   = m.get("metadata").collect { case x: Map[String, Any] => x }.getOrElse(Map.empty)
      )
    }
    val inboxData = rawState.get("inbox").collect { case m: Map[String, Any] => m }.getOrElse(Map.empty)
    val inbox = inboxData.map { case (taskId, taskData) =>
      val td = taskData.asInstanceOf[Map[String, Any]]
      taskId -> ApprovalTask(
        taskId          = td.getOrElse("task_id", taskId).toString,
        nodeId          = td.getOrElse("node_id", "").toString,
        token           = td.getOrElse("token", UUID.randomUUID().toString).toString,
        status          = td.getOrElse("status", "pending").toString,
        assignees       = td.get("assignees").collect { case l: List[_] => l.map(_.toString) }.getOrElse(Nil),
        assignedTo      = td.get("assigned_to").map(_.toString),
        waitingFor      = td.get("waiting_for").map(_.toString),
        dueAt           = td.get("due_at").collect { case s: String => java.time.Instant.parse(s) },
        escalationTarget = td.get("escalation_target").map(_.toString),
        createdAt       = td.get("created_at").collect { case s: String => java.time.Instant.parse(s) }.getOrElse(java.time.Instant.now()),
        resolvedAt      = td.get("resolved_at").collect { case s: String => java.time.Instant.parse(s) },
        metadata        = td.get("metadata").collect { case m: Map[String, Any] => m }.getOrElse(Map.empty)
      )
    }
    val state = WorkflowState(
      data             = rawState.get("data").collect { case m: Map[String, Any] => m }.getOrElse(Map.empty),
      schema           = rawState.get("schema").collect { case m: Map[String, Any] => m }.getOrElse(Map.empty),
      status           = rawState.getOrElse("status", "running").toString,
      graphVersion     = rawState.getOrElse("graph_version", "v1").toString,
      pendingPause     = pendingPause,
      inbox            = inbox,
      idempotencyCache = rawState.get("idempotency_cache").collect { case m: Map[String, Any] => m }.getOrElse(Map.empty),
      metadata         = rawState.get("metadata").collect { case m: Map[String, Any] => m }.getOrElse(Map.empty)
    )
    val rawTrace = data.get("trace").collect { case l: List[_] => l.map(_.asInstanceOf[Map[String, Any]]) }.getOrElse(Nil)
    val trace = rawTrace.map { item =>
      TraceEvent(
        eventType    = EventType.values.find(_.value == item.getOrElse("event_type", "").toString).getOrElse(EventType.Hop),
        sessionId    = item.getOrElse("session_id", data.getOrElse("session_id", "")).toString,
        nodeId       = item.getOrElse("node_id", "").toString,
        nodeName     = item.getOrElse("node_name", "").toString,
        timestamp    = item.get("timestamp").collect { case s: String => java.time.Instant.parse(s) }.getOrElse(java.time.Instant.now()),
        payload      = item.get("payload").collect { case m: Map[String, Any] => m }.getOrElse(Map.empty),
        eventId      = item.getOrElse("event_id", UUID.randomUUID().toString).toString,
        parentEventId = item.get("parent_event_id").map(_.toString),
        durationMs   = item.get("duration_ms").collect { case n: Number => n.longValue() }
      )
    }
    ExecutionContext(
      query         = data.getOrElse("query", ""),
      sessionId     = data.getOrElse("session_id", UUID.randomUUID().toString).toString,
      outputs       = data.get("outputs").collect { case m: Map[String, Any] => m }.getOrElse(Map.empty),
      trace         = trace,
      maxHops       = data.get("max_hops").collect { case n: Int => n; case n: Long => n.toInt }.getOrElse(20),
      hopCount      = data.get("hop_count").collect { case n: Int => n; case n: Long => n.toInt }.getOrElse(0),
      extraMessages = data.get("extra_messages").collect { case l: List[_] => l.map(_.asInstanceOf[Map[String, Any]]) }.getOrElse(Nil),
      state         = state,
      allowedTools  = data.get("allowed_tools").collect { case l: List[_] => Some(l.map(_.toString).toSet) }.flatten
    )
  }
}

// ---------------------------------------------------------------------------
// JSON utility for checkpoint serialization
// ---------------------------------------------------------------------------
// Visible package-wide: the backend layer reuses it to decode tool-call markup a
// local model wrote as text (see ToolCallRecovery).
private[engine] object JsonUtil {
  def toJson(value: Any): String = value match {
    case null              => "null"
    case s: String         =>
      val e = s.replace("\\", "\\\\").replace("\"", "\\\"")
               .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
      s""""$e""""
    case i: Int            => i.toString
    case l: Long           => l.toString
    case d: Double         => if (d.isNaN || d.isInfinite) "null" else d.toString
    case f: Float          => if (f.isNaN || f.isInfinite) "null" else f.toDouble.toString
    case b: Boolean        => b.toString
    case Some(v)           => toJson(v)
    case None              => "null"
    case list: List[_]     => s"[${list.map(toJson).mkString(",")}]"
    case list: Vector[_]   => s"[${list.map(toJson).mkString(",")}]"
    case map: Map[_, _]    =>
      val pairs = map.toList.map { case (k, v) => s"${toJson(k.toString)}:${toJson(v)}" }
      s"{${pairs.mkString(",")}}"
    case i: java.time.Instant => s""""${i.toString}""""
    case _                 =>
      val s = value.toString
      val e = s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
      s""""$e""""
  }

  def fromJson(json: String): Any = parseValue(json.trim, 0)._1

  @tailrec private def skipWS(s: String, pos: Int): Int =
    if (pos < s.length && s(pos).isWhitespace) skipWS(s, pos + 1) else pos

  private def parseValue(s: String, pos: Int): (Any, Int) = {
    val i = skipWS(s, pos)
    if (i >= s.length) throw new RuntimeException("unexpected end of JSON")
    s(i) match {
      case '"'                                     => parseString(s, i)
      case '{'                                     => parseObject(s, i)
      case '['                                     => parseArray(s, i)
      case 't'                                     => (true,  i + 4)
      case 'f'                                     => (false, i + 5)
      case 'n'                                     => (null,  i + 4)
      case '-' | '0' | '1' | '2' | '3' | '4' |
           '5' | '6' | '7' | '8' | '9'            => parseNumber(s, i)
      case c                                       => throw new RuntimeException(s"unexpected '$c' at $i")
    }
  }

  private def parseString(s: String, pos: Int): (String, Int) = {
    def escaped(i: Int): (Char, Int) = s(i) match {
      case '"'  => ('"',  i + 1)
      case '\\' => ('\\', i + 1)
      case 'n'  => ('\n', i + 1)
      case 'r'  => ('\r', i + 1)
      case 't'  => ('\t', i + 1)
      case 'u'  => (Integer.parseInt(s.substring(i + 1, i + 5), 16).toChar, i + 5)
      case c    => (c,    i + 1)
    }
    @tailrec def chars(i: Int, acc: List[Char]): (String, Int) =
      if (i >= s.length || s(i) == '"') (acc.reverse.mkString, i + 1)
      else if (s(i) == '\\') { val (c, next) = escaped(i + 1); chars(next, c :: acc) }
      else chars(i + 1, s(i) :: acc)
    chars(pos + 1, Nil)
  }

  private def parseNumber(s: String, pos: Int): (Any, Int) = {
    def digits(i: Int): Int = if (i < s.length && s(i).isDigit) digits(i + 1) else i
    val intEnd = digits(if (pos < s.length && s(pos) == '-') pos + 1 else pos)
    val isFloat = intEnd < s.length && (s(intEnd) == '.' || s(intEnd) == 'e' || s(intEnd) == 'E')
    val fracEnd = if (isFloat && s(intEnd) == '.') digits(intEnd + 1) else intEnd
    val end =
      if (fracEnd < s.length && (s(fracEnd) == 'e' || s(fracEnd) == 'E')) {
        val expStart = fracEnd + (if (fracEnd + 1 < s.length && (s(fracEnd + 1) == '+' || s(fracEnd + 1) == '-')) 2 else 1)
        digits(expStart)
      } else fracEnd
    if (isFloat) (s.substring(pos, end).toDouble, end)
    else {
      val num = s.substring(pos, end).toLong
      (if (num >= Int.MinValue && num <= Int.MaxValue) num.toInt else num, end)
    }
  }

  private def parseObject(s: String, pos: Int): (Map[String, Any], Int) = {
    def entries(i: Int, acc: List[(String, Any)]): (Map[String, Any], Int) = {
      val (key, i2)   = parseString(s, skipWS(s, i))
      val i3          = skipWS(s, i2)             // colon
      val (value, i4) = parseValue(s, skipWS(s, i3 + 1))
      val after       = skipWS(s, i4)
      if (after < s.length && s(after) == ',') entries(skipWS(s, after + 1), (key, value) :: acc)
      else ((key, value) :: acc).reverse.toMap -> (after + 1)
    }
    val i = skipWS(s, pos + 1)
    if (i < s.length && s(i) == '}') (Map.empty, i + 1) else entries(i, Nil)
  }

  private def parseArray(s: String, pos: Int): (List[Any], Int) = {
    def items(i: Int, acc: List[Any]): (List[Any], Int) = {
      val (value, i2) = parseValue(s, skipWS(s, i))
      val after       = skipWS(s, i2)
      if (after < s.length && s(after) == ',') items(skipWS(s, after + 1), value :: acc)
      else ((value :: acc).reverse, after + 1)
    }
    val i = skipWS(s, pos + 1)
    if (i < s.length && s(i) == ']') (Nil, i + 1) else items(i, Nil)
  }
}

case class ContextSelection(
  context: ContextNode,
  score: Double,
  source: String,
  hops: Int = 0,
  path: List[String] = Nil,
  reasons: List[String] = Nil,
  tokenCount: Int = 0
)

class ContextNavigator(
  val maxHops: Int = 2,
  val maxContextNodes: Int = 8,
  val maxContextTokens: Int = 4000,
  val semanticTopK: Int = 12,
  val perSourceLimit: Int = 2,
  val pathDecay: Double = 0.85,
  val tagWeight: Double = 0.15,
  val priorityWeight: Double = 0.1,
  val recencyWeight: Double = 0.1,
  val provenanceWeight: Double = 0.15,
  val staleFactPenalty: Double = 0.4,
  val excludeStaleFacts: Boolean = true
) {
  def navigate(
    store: GraphStore,
    agentNode: AgentNode,
    query: Option[String] = None,
    embedder: Option[Embedder] = None,
    sessionId: Option[String] = None
  ): ZIO[Any, Throwable, List[ContextSelection]] =
    for {
      directEdges <- store.getEdges(agentNode.nodeId, Some(EdgeType.HAS_CONTEXT))
      directSelections <- ZIO.foreach(directEdges) { edge =>
        store.getNode(edge.dstId).map { nodeOpt =>
          nodeOpt.collect {
            case ctx: ContextNode if ctx.isValid && (!excludeStaleFacts || ctx.isFactValid) =>
              ctx.nodeId -> scoreContext(ctx, Some(edge), query, None, "attached", 0, List(agentNode.nodeId, ctx.nodeId), edge.weight, sessionId)
          }
        }
      }.map(_.flatten)
      queryVec <- embedder match {
        case Some(e) => e.embedText(query.getOrElse("")).map(Some(_))
        case None    => ZIO.succeed(None)
      }
      semanticHits <- queryVec match {
        case Some(qv) => store.vectorSearch(qv, Some(List(NodeType.Context)), semanticTopK)
        case None     => ZIO.succeed(Nil)
      }
      directIds = directSelections.map(_._1).toSet
      semanticSelections = semanticHits.zipWithIndex.collect {
        case ((ctx: ContextNode, _), rank)
            if !directIds.contains(ctx.nodeId) && ctx.isValid && (!excludeStaleFacts || ctx.isFactValid) =>
          ctx.nodeId -> scoreContext(ctx, directEdges.find(_.dstId == ctx.nodeId), query, queryVec, "semantic", 0, List(ctx.nodeId), math.max(0.5, 1.0 - rank * 0.03), sessionId)
      }
      seedMap = (directSelections ++ semanticSelections).foldLeft(Map.empty[String, ContextSelection]) {
        case (acc, (id, sel)) => if (acc.contains(id)) acc else acc + (id -> sel)
      }
    } yield seedMap.values.toList.sortBy(-_.score).take(maxContextNodes)

  private def scoreContext(
    ctx: ContextNode,
    directEdge: Option[Edge],
    query: Option[String],
    queryVec: Option[List[Double]],
    source: String,
    hops: Int,
    path: List[String],
    pathWeight: Double,
    sessionId: Option[String]
  ): ContextSelection = {
    val semantic = queryVec.flatMap { q =>
      ctx.embedding.map { emb =>
        val normQ = normalize(q)
        val normE = normalize(emb)
        normQ.zip(normE).map { case (a, b) => a * b }.sum
      }
    }.getOrElse(0.5)
    
    val directAffinity = directEdge.map(_.weight).getOrElse(0.5)
    val pathBonus = math.pow(pathDecay, hops) * math.max(pathWeight, 0.1)
    val priorityBonus = ctx.priority * priorityWeight
    val recencyBonus = {
      val ageSeconds = (Instant.now().getEpochSecond - ctx.validAt.getEpochSecond)
      if (ageSeconds <= 3600) recencyWeight
      else if (ageSeconds <= 86400) recencyWeight / 2
      else 0.0
    }
    val provenanceBonus = if (ctx.attributes.get("origin") == Some("runtime")) provenanceWeight else 0.0
    val sessionBonus = if (sessionId.exists(_ == ctx.groupId)) provenanceWeight else 0.0

    val score = semantic * directAffinity * pathBonus +
      priorityBonus + recencyBonus + provenanceBonus + sessionBonus -
      (if (!ctx.isFactValid) staleFactPenalty else 0.0)

    val reasons = List(s"semantic=${f"$semantic%.2f"}", s"affinity=${f"$directAffinity%.2f"}", s"hops=$hops") ++
      (if (priorityBonus > 0) List(s"priority+=$priorityBonus") else Nil) ++
      (if (!ctx.isFactValid) List(s"stale-=$staleFactPenalty") else Nil)
    
    ContextSelection(
      context = ctx,
      score = score,
      source = source,
      hops = hops,
      path = path,
      reasons = reasons.toList,
      tokenCount = if (ctx.tokenCount > 0) ctx.tokenCount else math.max(1, ctx.content.split("\\s+").length + ctx.content.length / 12)
    )
  }

  private def normalize(vec: List[Double]): List[Double] = {
    val norm = math.sqrt(vec.map(x => x * x).sum)
    if (norm == 0) vec else vec.map(x => x / norm)
  }
}

case class ComposedAgent(
  agentNode: AgentNode,
  tools: List[ToolNode],
  context: List[ContextNode],
  contextSelection: List[ContextSelection],
  prompt: Option[PromptNode],
  delegates: List[AgentNode]
) {
  def buildSystemPrompt(promptVars: Map[String, Any] = Map.empty): String = {
    val parts = scala.collection.mutable.ListBuffer[String]()
    
    prompt match {
      case Some(p) => parts += p.render(promptVars)
      case None if agentNode.systemPrompt.nonEmpty => parts += agentNode.systemPrompt
      case _ =>
    }
    
    if (context.nonEmpty) {
      parts += "\n\n## Relevant Context\n"
      for (ctx <- context) {
        val header = if (ctx.name.nonEmpty) s"### ${ctx.name}" else "###"
        val sourcePart = if (ctx.source.nonEmpty) s" (source: ${ctx.source})" else ""
        parts += s"$header$sourcePart\n${ctx.content}"
      }
    }
    
    parts.mkString("\n")
  }

  def buildToolSchemas: List[Map[String, Any]] = tools.map(_.toToolSchema)
}

class AgentComposer(
  store: GraphStore,
  embedder: Option[Embedder] = None,
  contextNavigator: ContextNavigator = ContextNavigator()
) {
  def compose(agentNode: AgentNode, query: Option[String] = None, sessionId: Option[String] = None): ZIO[Any, Throwable, ComposedAgent] =
    for {
      toolEdges <- store.getEdges(agentNode.nodeId, Some(EdgeType.HAS_TOOL))
      toolNodes <- ZIO.foreach(toolEdges.sortBy(-_.weight)) { edge =>
        store.getNode(edge.dstId).map {
          _.collect { case n: ToolNode if n.isValid => n }
        }
      }.map(_.flatten)
      contextSelection <- contextNavigator.navigate(store, agentNode, query, embedder, sessionId)
      promptNodes <- store.neighbors(agentNode.nodeId, Some(EdgeType.HAS_PROMPT))
      delegateNodes <- store.neighbors(agentNode.nodeId, Some(EdgeType.DELEGATES_TO))
    } yield ComposedAgent(
      agentNode = agentNode,
      tools = toolNodes,
      context = contextSelection.map(_.context),
      contextSelection = contextSelection,
      prompt = promptNodes.collect { case n: PromptNode => n }.headOption,
      delegates = delegateNodes.collect { case n: AgentNode if n.isValid => n }
    )
}

// How many times each bounded in-turn recovery has already fired for the agent step
// currently running. Each is capped independently so a model that cannot comply with
// one correction still terminates instead of looping to maxIterations.
private case class LoopGuards(
  toolCallCorrections: Int = 0,
  continuationNudges: Int = 0,
  completionNudges: Int = 0
)

object GraphExecutor {
  /** An integer knob read at USE time, not at class-initialisation time.
    *
    * These used to be `val`s, so they were baked in the first time the class loaded,
    * which silently ignored any host that configured the engine later — and a host
    * naturally does exactly that, because the right cap depends on the model it just
    * picked. A server that loads the engine at startup and then asks for a 13200-token
    * ceiling per run kept escalating to the 32000 default: it asked a 22000-token-context
    * model for 32000 output tokens, which can never return, so the turn truncated,
    * escalated, and truncated again — roughly 22 minutes per doomed attempt with no way
    * for the host to intervene. Nothing failed loudly; the run just crawled.
    *
    * The JVM's environment is fixed for the life of the process, so the live
    * channel here is a system property: `LOKI_TOKEN_ESCALATION_CAP` is also
    * readable as `loki.token.escalation.cap`, and the property wins. The
    * environment variable keeps working and the default is unchanged, so a process that
    * sets neither behaves exactly as before.
    */
  private[engine] def intKnob(envName: String, default: Int): Int =
    sys.props.get(envName.toLowerCase.replace('_', '.'))
      .orElse(sys.env.get(envName))
      .flatMap(_.trim.toIntOption)          // a malformed knob must not kill the run
      .getOrElse(default)

  private[engine] def MaxTokenEscalations: Int = intKnob("LOKI_MAX_TOKEN_ESCALATIONS", 2)
  private[engine] def TokenEscalationFactor: Int = intKnob("LOKI_TOKEN_ESCALATION_FACTOR", 2)
  private[engine] def TokenEscalationCap: Int = intKnob("LOKI_TOKEN_ESCALATION_CAP", 32000)

  // A local model that writes a tool call as TEXT instead of through the tool API (the
  // server's parser didn't recognise its format) produces a turn that looks like a final
  // answer but did no work — an agent loop would end mid-task. Same treatment as a
  // max_tokens truncation: tell the model its call wasn't parsed and let it retry, rather
  // than accepting the markup as the result. Bounded, so a model that cannot produce a
  // well-formed call still terminates instead of looping to maxIterations.
  // 0 disables the correction (the turn is then terminal, as before).
  private[engine] def MaxToolCallCorrections: Int = intKnob("LOKI_MAX_TOOL_CALL_CORRECTIONS", 3)
  // A final turn that is BLANK or reads as a narration lead-in ("Now let me resolve all
  // the field contexts:") is the model announcing its NEXT action, not finishing. With no
  // parseable tool call the loop accepted it as the answer and the agent stopped mid-task:
  // four aggregator shards ended at 12/23/5/12 of a 100-turn budget, their final text
  // either blank or exactly such a lead-in — none had written its output. Nudge (bounded)
  // rather than accept a non-answer as the result. 0 restores the old behaviour.
  private[engine] def MaxContinuationNudges: Int = intKnob("LOKI_MAX_CONTINUATION_NUDGES", 2)
  // An agent whose work is DEFINED by artefacts it must leave behind has not finished
  // while they are absent, however confident its closing message sounds. A consumer
  // declares them on the node as `attributes("completion_paths")` (paths, absolute or
  // relative to the process working directory); a terminal turn with any of them missing
  // is treated as not-done and the work is asked for again. Generic on purpose — the
  // engine only checks existence, it never interprets the contents. 0 disables the check.
  private[engine] def MaxCompletionNudges: Int = intKnob("LOKI_MAX_COMPLETION_NUDGES", 2)
  // The symmetric complement: when EVERY declared path already exists before the first
  // turn, skip the agent instead of spending a turn discovering it has nothing to do.
  private[engine] def skipCompletedAgents: Boolean =
    !sys.props.get("loki.skip.completed.agents")
      .orElse(sys.env.get("LOKI_SKIP_COMPLETED_AGENTS"))
      .contains("0")

  private[engine] val ToolCallCorrectionPrompt =
    "Your previous message contained a tool call written as plain text, so it was NOT " +
    "executed — nothing ran. Do not describe or hand-write tool calls in your message " +
    "content. Re-issue that single call using the tool-calling API, with the arguments " +
    "as a JSON object. If you meant to give a final answer instead, reply with the " +
    "answer and no tool-call markup."

  private[engine] val ContinuationNudge =
    "Your last message did not finish the task and issued no tool call — it read as an " +
    "announcement of what you were about to do. Continue now: either make the next tool " +
    "call, or, if the work is genuinely complete, state the final result explicitly."

  private[engine] def completionNudge(missing: List[String]): String =
    "You have not finished: the file(s) this task must produce do not exist yet — " +
    missing.mkString(", ") + ". Write them now, then stop. " +
    "Do not describe what you would write; write it."

  // A lead-in announces an action and stops: trailing colon, plus an opening like
  // "Now let me …" / "Next, I'll …" / "Let me …". Kept narrow so a real answer that
  // happens to end in a colon-led list is not mistaken for one (it would have content
  // after the colon).
  private val NarrationLeadIn =
    """(?ix)^\s*( (now\s+)?let(\s+me|'s)\b | next\s*[,:] | i\s*(will|'ll)\b | (first|then|finally)\s*[,:] )""".r

  /** Whether a no-tool-call turn is an announcement rather than an answer.
    *
    * True for blank/whitespace output, and for a short lead-in that ends on a colon
    * (the model was about to act). A substantive answer — anything of length that does
    * not end mid-announcement — is left alone. A blank `text` whose reasoning channel
    * reached a real conclusion is also left alone: the executor salvages that. */
  private[engine] def looksLikeNarrationOnly(text: String, reasoning: String = ""): Boolean = {
    val body = Option(text).getOrElse("").trim
    if (body.isEmpty) Option(reasoning).getOrElse("").trim.isEmpty
    else if (body.length > 400) false       // substantive output, not a lead-in
    else body.endsWith(":") && NarrationLeadIn.findFirstIn(body).isDefined
  }

  /** The `completion_paths` a node declares, normalised to a list. */
  private[engine] def declaredCompletionPaths(node: Node): List[String] =
    node.attributes.get("completion_paths") match {
      case Some(single: String)   => List(single).filter(_.nonEmpty)
      case Some(many: Iterable[?]) => many.toList.map(String.valueOf).filter(_.nonEmpty)
      case Some(many: Array[?])    => many.toList.map(String.valueOf).filter(_.nonEmpty)
      case _                       => Nil
    }

  /** Declared `completion_paths` for `node` that do not exist yet. */
  private[engine] def missingCompletionPaths(node: Node): List[String] =
    declaredCompletionPaths(node).filterNot { path =>
      try java.nio.file.Files.exists(java.nio.file.Paths.get(path))
      catch { case scala.util.control.NonFatal(_) => false }  // unusable path == not there
    }

  // ---------------------------------------------------------------------------
  // Schema-directed coercion of tool inputs
  // ---------------------------------------------------------------------------
  // Small local models routinely emit the RIGHT arguments in the WRONG JSON type —
  // most often an array serialised as a comma-separated string
  // (`{"actions": "show,hide"}` for `actions: {type: "array"}`), and scalars as
  // strings (`{"limit": "5"}`). Input validation runs before the tool is called, so
  // such a turn dies as `tool_input.actions must be an array` and the work is lost even
  // though the model's INTENT was unambiguous and the fix mechanical.
  //
  // Coerce only where the declared schema states the target type, and only from a
  // lossless textual form. Anything already the right type is untouched; nothing is
  // invented; an undeclared key is left exactly as sent (validation still judges it).
  private val TrueTokens  = Set("true", "yes", "1", "on")
  private val FalseTokens = Set("false", "no", "0", "off")

  /** Best-effort scalar coercion; returns `value` unchanged when unsure. */
  private def coerceScalar(value: Any, target: String): Any = value match {
    case s: String =>
      val text = s.trim
      target match {
        case "integer" =>
          text.toIntOption.map(v => v: Any).orElse(text.toLongOption.map(v => v: Any)).getOrElse(value)
        case "number"  => text.toDoubleOption.map(v => v: Any).getOrElse(value)
        case "boolean" =>
          val low = text.toLowerCase
          if (TrueTokens.contains(low)) true
          else if (FalseTokens.contains(low)) false
          else value
        case _ => value
      }
    case _ => value
  }

  private def coerceItems(items: List[Any], spec: Map[String, Any]): List[Any] =
    spec.get("items").collect { case m: Map[?, ?] => m.asInstanceOf[Map[String, Any]] }
      .flatMap(_.get("type")).map(_.toString) match {
        case Some(t @ ("integer" | "number" | "boolean")) => items.map(coerceScalar(_, t))
        case _                                            => items
      }

  private def coerceValue(value: Any, spec: Map[String, Any]): Any =
    spec.get("type").map(_.toString) match {
      case Some("array") => value match {
        case _: Seq[?] => value
        case null      => value
        case s: String => coerceItems(s.split(",", -1).toList.map(_.trim).filter(_.nonEmpty), spec)
        case other     => coerceItems(List(other), spec)  // a lone scalar for a list-typed param
      }
      case Some(t @ ("integer" | "number" | "boolean")) => coerceScalar(value, t)
      case _                                            => value
    }

  /** Return `payload` with values nudged to the types its schema declares.
    *
    * Handles the shapes a local model actually gets wrong: a comma-separated (or
    * single) string where an array is declared, and a stringified number/bool where a
    * scalar is declared. Returns the SAME map when nothing needed changing. */
  private[engine] def coerceToSchema(payload: Map[String, Any], schema: Map[String, Any]): Map[String, Any] = {
    val props = schema.get("properties").collect { case m: Map[?, ?] => m.asInstanceOf[Map[String, Any]] }
    props match {
      case None => payload
      case Some(properties) =>
        payload.foldLeft(payload) { case (acc, (key, value)) =>
          properties.get(key).collect { case m: Map[?, ?] => m.asInstanceOf[Map[String, Any]] } match {
            case Some(spec) =>
              val coerced = coerceValue(value, spec)
              if (coerced.asInstanceOf[AnyRef] eq value.asInstanceOf[AnyRef]) acc else acc.updated(key, coerced)
            case None => acc
          }
        }
    }
  }

  private[engine] val HistoryCompactEvery: Int = sys.env.get("LOKI_HISTORY_COMPACT_EVERY").flatMap(_.toIntOption).getOrElse(6)
  private[engine] val HistoryHeadKeep: Int = sys.env.get("LOKI_HISTORY_HEAD_KEEP").flatMap(_.toIntOption).getOrElse(2)
  private[engine] val HistoryRecentKeep: Int = sys.env.get("LOKI_HISTORY_RECENT_KEEP").flatMap(_.toIntOption).getOrElse(6)
  private[engine] val HistoryElideChars: Int = sys.env.get("LOKI_HISTORY_ELIDE_CHARS").flatMap(_.toIntOption).getOrElse(600)
  private[engine] val HistoryElideMark = "…[history-elided]"

  private[engine] def clipForHistory(text: String): String =
    if (text.length <= HistoryElideChars || text.contains(HistoryElideMark)) text
    else s"${text.take(HistoryElideChars)} $HistoryElideMark (+${text.length - HistoryElideChars} chars)"

  private def shrinkBlock(block: Map[String, Any]): Map[String, Any] =
    block.get("type").map(_.toString) match {
      case Some("text") =>
        block.get("text") match {
          case Some(text: String) => block.updated("text", clipForHistory(text))
          case _                  => block
        }
      case Some("tool_result") =>
        block.get("content") match {
          case Some(text: String) => block.updated("content", clipForHistory(text))
          case Some(items: List[?]) =>
            val shrunk = items.map {
              case item: Map[?, ?] =>
                val typed = item.asInstanceOf[Map[String, Any]]
                typed.get("text") match {
                  case Some(text: String) => typed.updated("text", clipForHistory(text))
                  case _                  => typed
                }
              case item => item
            }
            block.updated("content", shrunk)
          case _ => block
        }
      case _ => block // Preserve tool_use/tool_call blocks so API pairing remains valid.
    }

  private def shrinkMessage(message: Map[String, Any]): Map[String, Any] =
    message.get("content") match {
      case Some(text: String) => message.updated("content", clipForHistory(text))
      case Some(blocks: List[?]) =>
        val shrunk = blocks.map {
          case block: Map[?, ?] => shrinkBlock(block.asInstanceOf[Map[String, Any]])
          case block            => block
        }
        message.updated("content", shrunk)
      case _ => message
    }

  private[engine] def compactHistory(messages: List[Map[String, Any]]): List[Map[String, Any]] = {
    val middleEnd = messages.length - HistoryRecentKeep
    if (messages.length <= HistoryHeadKeep + HistoryRecentKeep) messages
    else messages.zipWithIndex.map { case (message, index) =>
      if (index >= HistoryHeadKeep && index < middleEnd) shrinkMessage(message) else message
    }
  }
}

class GraphExecutor(
  val store: GraphStore,
  val backend: LLMBackend,
  val composer: AgentComposer,
  val routerModel: String = "claude-haiku-4-5-20251001"
) {
  private val toolFns = mutable.Map[String, Any => Any]()
  private val eventHooks = new AtomicReference[List[(TraceEvent, ExecutionContext) => Any]](Nil)
  private val runTraces = TrieMap.empty[String, ConcurrentLinkedQueue[TraceEvent]]

  /** A pause raised mid-run that intermediate contexts don't carry; applied to
    * the final context so callers observe the paused state. */
  private val pendingPause = new AtomicReference[Option[WorkflowPause]](None)

  def registerTool(callableRef: String, fn: Any => Any): Unit = {
    toolFns.put(callableRef, fn)
  }

  def addEventHook(fn: (TraceEvent, ExecutionContext) => Any): Unit = {
    eventHooks.updateAndGet(hooks => fn :: hooks)
  }

  private def emit(ctx: ExecutionContext, eventType: EventType, nodeId: String, nodeName: String, payload: Map[String, Any], parentEventId: Option[String] = None, durationMs: Option[Long] = None): TraceEvent = {
    val event = TraceEvent(eventType, ctx.sessionId, nodeId, nodeName, Instant.now(), payload, parentEventId = parentEventId, durationMs = durationMs)
    runTraces.get(ctx.sessionId).foreach(_.add(event))
    eventHooks.get.foreach { fn => try fn(event, ctx) catch { case scala.util.control.NonFatal(_) => () } }
    event
  }

  def run(
    entryNodeId: String,
    query: Any,
    strategy: String = "sequential",
    maxHops: Int = 20,
    extraMessages: Option[List[Map[String, Any]]] = None,
    executionContext: Option[ExecutionContext] = None,
    state: Option[Map[String, Any]] = None,
    allowedTools: Option[List[String]] = None,
    options: Option[ExecutionOptions] = None
  ): ZIO[Any, Throwable, ExecutionContext] = {
    val ctx = executionContext.getOrElse(ExecutionContext(query = query, maxHops = maxHops))
    val updatedCtx = ctx.copy(
      query = if (query != null) query else ctx.query,
      maxHops = maxHops,
      extraMessages = extraMessages.getOrElse(ctx.extraMessages),
      state = state.fold(ctx.state)(s => ctx.state.copy(data = ctx.state.data ++ s)),
      allowedTools = allowedTools.map(_.toSet).orElse(ctx.allowedTools).orElse(options.flatMap(_.allowedTools))
    )
    val runTrace = new ConcurrentLinkedQueue[TraceEvent]()

    ZIO.acquireReleaseWith(
      ZIO.succeed(runTraces.put(updatedCtx.sessionId, runTrace))
    )(_ => ZIO.succeed(runTraces.remove(updatedCtx.sessionId)).unit) { _ =>
      pendingPause.set(None)
      for {
        entryOpt <- store.getNode(entryNodeId)
        result <- entryOpt match {
          case Some(entry) =>
            strategy match {
              case "sequential" => runSequential(entry, updatedCtx)
              case "parallel" => runParallel(entry, updatedCtx)
              case "topological" => runTopological(entryNodeId, updatedCtx)
              case _ => ZIO.fail(new IllegalArgumentException(s"Unknown strategy: $strategy"))
            }
          case None => ZIO.fail(new IllegalArgumentException(s"Entry node $entryNodeId not found"))
        }
      } yield {
        // A pause raised mid-run (e.g. by executeApproval) is recorded in
        // pendingPause but the intermediate contexts don't carry it; apply
        // it here so callers observe the paused state.
        val finalCtx = (pendingPause.get, result.state.pendingPause) match {
          case (Some(p), None) => result.copy(state = result.state.copy(status = "paused", pendingPause = Some(p)))
          case _               => result
        }
        pendingPause.set(None)
        finalCtx.copy(trace = updatedCtx.trace ++ runTrace.iterator().asScala.toList)
      }
    }
  }

  def resume(
    entryNodeId: String,
    ctx: ExecutionContext,
    query: Option[Any] = None,
    strategy: String = "sequential"
  ): ZIO[Any, Throwable, ExecutionContext] = {
    val updatedCtx = ctx.copy(
      query = query.getOrElse(ctx.query),
      state = ctx.state.copy(status = "running", pendingPause = None)
    )
    run(entryNodeId, updatedCtx.query, strategy, ctx.maxHops, executionContext = Some(updatedCtx))
  }

  private def runSequential(node: AnyNode, ctx: ExecutionContext): ZIO[Any, Throwable, ExecutionContext] = {
    if (ctx.isPaused || ctx.hopCount >= ctx.maxHops) {
      ZIO.succeed(ctx)
    } else {
      val newCtx = ctx.copy(hopCount = ctx.hopCount + 1)
      val hopEvent = emit(newCtx, EventType.Hop, node.nodeId, node.name, Map(
        "hop" -> newCtx.hopCount,
        "node_type" -> node.nodeType.toString,
        "summary" -> ""
      ))
      
      for {
        result <- executeNode(node, newCtx, Some(hopEvent.eventId))
        ctxWithOutput = newCtx.copy(outputs = newCtx.outputs + (node.nodeId -> result))
        outputKeyData = node match {
          case t: TransformNode if t.outputKey.nonEmpty => Map(t.outputKey -> result)
          case r: ReasonerNode  if r.outputKey.nonEmpty => Map(r.outputKey -> result)
          case _ => Map.empty[String, Any]
        }
        ctxWithState = ctxWithOutput.copy(state = ctxWithOutput.state.copy(data = ctxWithOutput.state.data + ("_last_node_id" -> node.nodeId, "_last_output" -> result) ++ outputKeyData))
        
        finalCtx <- ctxWithState.state.pendingPause match {
          case Some(pause) =>
            pendingPause.set(Some(pause))
            ZIO.succeed(ctxWithState)
          case None =>
            node match {
              case a: AgentNode if a.routingTable.nonEmpty =>
                for {
                  (nextId, routingEvent) <- routeNode(a, result, ctxWithState, Some(hopEvent.eventId))
                  ctxWithRoute = ctxWithState.withTrace(routingEvent)
                  ctxWithUpdatedTrace = ctxWithRoute.copy(trace = hopEvent :: ctxWithRoute.trace)
                  ctxFinal <- if (nextId != "__END__" && nextId.nonEmpty) {
                    store.getNode(nextId).flatMap {
                      case Some(nextNode) => runSequential(nextNode, ctxWithUpdatedTrace)
                      case None => ZIO.succeed(ctxWithUpdatedTrace)
                    }
                  } else {
                    ZIO.succeed(ctxWithUpdatedTrace.copy(state = ctxWithUpdatedTrace.state.copy(status = "completed")))
                  }
                } yield ctxFinal
              case _: ApprovalNode =>
                for {
                  approvalResult <- executeApproval(node.asInstanceOf[ApprovalNode], ctxWithState)
                  ctxWithApproval = ctxWithState.copy(outputs = ctxWithState.outputs + (node.nodeId -> approvalResult))
                  // next_node_id is None (not a String) while the approval is pending
                  ctxFinal <- approvalResult.get("next_node_id").collect { case s: String => s } match {
                    case Some(nextId) if nextId != "__END__" && nextId.nonEmpty =>
                      store.getNode(nextId).flatMap {
                        case Some(nextNode) => runSequential(nextNode, ctxWithApproval)
                        case None => ZIO.succeed(ctxWithApproval)
                      }
                    case _ => ZIO.succeed(ctxWithApproval)
                  }
                } yield ctxFinal
              case _ =>
                ZIO.succeed(ctxWithState)
            }
        }
      } yield finalCtx
    }
  }

  private def runParallel(node: AnyNode, ctx: ExecutionContext): ZIO[Any, Throwable, ExecutionContext] = {
    if (ctx.isPaused || ctx.hopCount >= ctx.maxHops) {
      ZIO.succeed(ctx)
    } else {
      val newCtx = ctx.copy(hopCount = ctx.hopCount + 1)
      val hopEvent = emit(newCtx, EventType.Hop, node.nodeId, node.name, Map(
        "hop" -> newCtx.hopCount,
        "node_type" -> node.nodeType.toString,
        "summary" -> ""
      ))
      
      for {
        result <- executeNode(node, newCtx)
        ctxWithOutput = newCtx.copy(outputs = newCtx.outputs + (node.nodeId -> result))
        
        delegateEdges <- store.getEdges(node.nodeId, Some(EdgeType.DELEGATES_TO))
        delegateResults <- ZIO.foreach(delegateEdges) { edge =>
          store.getNode(edge.dstId).flatMap {
            case Some(n: AgentNode) => runSequential(n, ctxWithOutput)
            case _ => ZIO.succeed(ctxWithOutput)
          }
        }
      } yield ctxWithOutput.copy(outputs = ctxWithOutput.outputs + (node.nodeId -> Map(
        "node_result" -> result,
        "delegate_results" -> delegateEdges.map(_.dstId).zip(delegateResults.map(_.outputs)).toMap
      )))
    }
  }

  private def runTopological(entryNodeId: String, ctx: ExecutionContext): ZIO[Any, Throwable, ExecutionContext] = {
    def buildDepMap(visited: Set[String], depMap: Map[String, Set[String]], queue: List[String]): ZIO[Any, Throwable, (Map[String, Set[String]], List[String])] =
      queue match {
        case Nil => ZIO.succeed((depMap, visited.toList))
        case nid :: rest if visited.contains(nid) => buildDepMap(visited, depMap, rest)
        case nid :: rest =>
          store.getEdges(nid, Some(EdgeType.DELEGATES_TO)).flatMap { edges =>
            val newVisited = visited + nid
            val newDepMap = edges.foldLeft(depMap) { (dm, edge) =>
              dm.updated(edge.dstId, dm.getOrElse(edge.dstId, Set.empty) + nid)
            }
            val newQueue = edges.map(_.dstId).filterNot(newVisited.contains) ::: rest
            buildDepMap(newVisited, newDepMap, newQueue)
          }
      }

    buildDepMap(Set.empty, Map.empty, List(entryNodeId)).flatMap { case (rawDepMap, allNodeIds) =>
      // Second pass: add TransformNode.inputKeys as explicit fan-in dependencies.
      ZIO.foldLeft(allNodeIds)(rawDepMap) { (dm, nid) =>
        store.getNode(nid).map {
          case Some(t: TransformNode) if t.inputKeys.nonEmpty =>
            t.inputKeys.foldLeft(dm) { (m, key) =>
              if (m.contains(key)) m.updated(nid, m.getOrElse(nid, Set.empty) + key) else m
            }
          case _ => dm
        }
      }.map(_ => allNodeIds)
    }.flatMap { allNodeIds =>
      val ready = allNodeIds

      def executeWave(nodes: List[AnyNode], c: ExecutionContext): ZIO[Any, Throwable, ExecutionContext] = {
        if (c.isPaused || c.hopCount >= c.maxHops || nodes.isEmpty) {
          ZIO.succeed(c)
        } else {
          for {
            results <- ZIO.foreach(nodes) { n =>
              val newCtx = c.copy(hopCount = c.hopCount + 1)
              val hopEvent = emit(newCtx, EventType.Hop, n.nodeId, n.name, Map(
                "hop" -> newCtx.hopCount,
                "node_type" -> n.nodeType.toString,
                "summary" -> ""
              ))
              executeNode(n, newCtx, Some(hopEvent.eventId)).map(r => (n, r, hopEvent))
            }
            ctxWithOutputs = results.foldLeft(c) { (acc, triple) =>
              val (n, r, _) = triple
              val outputKeyData = n match {
                case t: TransformNode if t.outputKey.nonEmpty => Map(t.outputKey -> r)
                case r2: ReasonerNode if r2.outputKey.nonEmpty => Map(r2.outputKey -> r)
                case _ => Map.empty[String, Any]
              }
              acc.copy(
                outputs = acc.outputs + (n.nodeId -> r),
                state = if (outputKeyData.nonEmpty) acc.state.copy(data = acc.state.data ++ outputKeyData) else acc.state
              )
            }
          } yield ctxWithOutputs
        }
      }

      for {
        initialNodes <- ZIO.foreach(ready.distinct.take(ctx.maxHops - ctx.hopCount))(id => store.getNode(id))
        validNodes = initialNodes.flatten
        resultCtx <- executeWave(validNodes, ctx)
      } yield resultCtx
    }
  }

  private def executeNode(node: AnyNode, ctx: ExecutionContext, parentEventId: Option[String] = None): ZIO[Any, Throwable, Any] = {
    node match {
      case a: AgentNode    => executeAgent(a, ctx)
      case t: ToolNode     =>
        // Emit tool_result for top-level ToolNode steps. executeTool itself
        // does not emit (agent inner-tool calls go through handleToolCalls,
        // which emits its own tool_result); without this the step hook never
        // marks a pipeline ToolNode done, freezing progress UIs at "0 steps".
        val t0 = java.lang.System.currentTimeMillis()
        executeTool(t, ctx).map { result =>
          val durationMs = java.lang.System.currentTimeMillis() - t0
          emit(ctx, EventType.ToolResult, t.nodeId, t.name, Map(
            "tool_name" -> t.name,
            "callable_ref" -> t.callableRef,
            "output_summary" -> summarize(result),
            "success" -> true,
            "duration_ms" -> durationMs
          ), parentEventId)
          result
        }
      case c: ContextNode  => ZIO.succeed(c.content)
      case p: PromptNode   => ZIO.succeed(p.render(Map.empty))
      case g: GraphNode    => executeSubgraph(g, ctx, parentEventId)
      case a: ApprovalNode => executeApproval(a, ctx)
      case t: TransformNode => executeTransform(t, ctx)
      case r: ReasonerNode => executeReasoner(r, ctx, parentEventId)
      case _               => ZIO.succeed(None)
    }
  }

  private val MaxSubgraphDepth = 16

  private def executeSubgraph(node: GraphNode, ctx: ExecutionContext, parentEventId: Option[String] = None): ZIO[Any, Throwable, Any] =
    if (ctx.activeSubgraphs.contains(node.nodeId))
      ZIO.fail(new IllegalStateException(
        s"GraphNode cycle detected: ${(ctx.activeSubgraphs :+ node.nodeId).mkString(" → ")}"
      ))
    else if (ctx.activeSubgraphs.length >= MaxSubgraphDepth)
      ZIO.fail(new IllegalStateException(
        s"GraphNode recursion depth exceeded ($MaxSubgraphDepth) at '${node.name}'"
      ))
    else if (node.entryNodeId.isEmpty)
      ZIO.fail(new IllegalArgumentException(s"GraphNode '${node.name}' has no entryNodeId"))
    else {
    val exitId   = if (node.exitNodeId.nonEmpty) node.exitNodeId else node.entryNodeId
    val subQuery = resolveSubgraphQuery(node, ctx)
    val subState = resolveSubgraphState(node, ctx)

    val enterPayload = Map(
      "entry_node_id" -> node.entryNodeId,
      "exit_node_id"  -> exitId,
      "strategy"      -> node.strategy,
      "scoped"        -> node.scopeOutputs
    )
    emit(ctx, EventType.SubgraphEnter, node.nodeId, node.name, enterPayload, parentEventId)

    def runSubgraph(): ZIO[Any, Throwable, Any] =
      store.getNode(node.entryNodeId).flatMap {
        case None =>
          ZIO.fail(new IllegalArgumentException(
            s"GraphNode '${node.name}' entryNodeId '${node.entryNodeId}' not found"
          ))
        case Some(subEntry) if node.scopeOutputs =>
          val child = ctx.copy(
            query          = subQuery,
            outputs        = Map.empty,
            trace          = Nil,
            maxHops        = math.max(ctx.maxHops - ctx.hopCount, 1),
            hopCount       = 0,
            state          = ctx.state.copy(data = ctx.state.data ++ subState),
            activeSubgraphs = ctx.activeSubgraphs :+ node.nodeId
          )
          dispatchStrategy(node.strategy, subEntry, child).map { childCtx =>
            childCtx.outputs.get(exitId)
          }
        case Some(subEntry) =>
          val ctxWithState = if (subState.nonEmpty)
            ctx.copy(state = ctx.state.copy(data = ctx.state.data ++ subState),
                     activeSubgraphs = ctx.activeSubgraphs :+ node.nodeId)
          else ctx.copy(activeSubgraphs = ctx.activeSubgraphs :+ node.nodeId)
          val prevQuery = ctxWithState.query
          dispatchStrategy(node.strategy, subEntry, ctxWithState.copy(query = subQuery)).map { resultCtx =>
            resultCtx.outputs.get(exitId)
          }
      }

    callWithRetry(() => runSubgraph(), ctx, node, node.executionPolicy)
      .tapBoth(
        err => ZIO.succeed(emit(ctx, EventType.SubgraphExit, node.nodeId, node.name,
          Map("exit_node_id" -> exitId, "summary" -> "", "error" -> err.getMessage), parentEventId)),
        result => ZIO.succeed(emit(ctx, EventType.SubgraphExit, node.nodeId, node.name,
          Map("exit_node_id" -> exitId, "summary" -> summarize(result)), parentEventId))
      )
  }

  private def dispatchStrategy(strategy: String, entry: AnyNode, ctx: ExecutionContext): ZIO[Any, Throwable, ExecutionContext] =
    strategy match {
      case "sequential"  => runSequential(entry, ctx)
      case "parallel"    => runParallel(entry, ctx)
      case "topological" => runTopological(entry.nodeId, ctx)
      case _             => ZIO.fail(new IllegalArgumentException(s"Unknown sub-graph strategy: $strategy"))
    }

  private def resolveSubgraphQuery(node: GraphNode, ctx: ExecutionContext): Any =
    if (node.inputKeys.isEmpty) ctx.query
    else {
      val parts = node.inputKeys.flatMap { key =>
        ctx.outputs.get(key).orElse(ctx.state.data.get(key)).map(textOf)
      }.filter(_.nonEmpty)
      if (parts.isEmpty) ctx.query else parts.mkString("\n\n")
    }

  private def resolveSubgraphState(node: GraphNode, ctx: ExecutionContext): Map[String, Any] =
    node.inputMap.flatMap { case (alias, source) =>
      ctx.outputs.get(source).orElse(ctx.state.data.get(source)).map(alias -> _)
    }

  private def executeAgent(node: AgentNode, ctx: ExecutionContext): ZIO[Any, Throwable, Map[String, Any]] = {
    val t0 = java.lang.System.currentTimeMillis()

    // Pre-satisfied completion_paths → the node's work already exists; skip the agent
    // entirely. The symmetric complement of the not-done check in the agent loop (a
    // terminal turn with a declared path MISSING is re-prompted): on resume, a fan-out
    // re-executes every agent node, and each "already done" discovery costs a full LLM
    // turn — minutes per no-op on an always-thinking local model, or a whole
    // re-invocation on a per-call-priced backend. Prompt-level idempotency guards
    // cannot close this: a lean tool belt has no file-existence tool, so the
    // instruction is unsatisfiable — the engine is the only place that can check.
    // Same contract as the not-done side: existence only, contents never interpreted.
    val declaredPaths = GraphExecutor.declaredCompletionPaths(node)
    if (declaredPaths.nonEmpty && GraphExecutor.skipCompletedAgents &&
        GraphExecutor.missingCompletionPaths(node).isEmpty) {
      ZIO.succeed {
        emit(ctx, EventType.AgentSkip, node.nodeId, node.name, Map(
          "reason" -> "completion_paths_already_satisfied",
          "paths" -> declaredPaths
        ))
        Map[String, Any](
          "text" -> "already complete: every declared completion path exists",
          "intent" -> "complete",
          "skipped" -> true
        )
      }
    } else
    for {
      composed <- composer.compose(node, Some(queryText(ctx.query)), Some(ctx.sessionId))
      systemPrompt = composed.buildSystemPrompt()
      toolDefs = composed.buildToolSchemas

      stateSchema = if (node.stateSchema.nonEmpty) node.stateSchema else ctx.state.schema
      ctxWithSchema = if (stateSchema.nonEmpty) ctx.copy(state = ctx.state.copy(schema = stateSchema)) else ctx

      agentEventId = UUID.randomUUID().toString()
      _ = emitAgentStart(node, composed, systemPrompt, ctxWithSchema, agentEventId)

      messages = (ctx.extraMessages ::: ctx.state.data.get("_workflow_state").map { ws =>
        Map("role" -> "assistant", "content" -> s"Workflow state:\n$ws")
      }.toList ::: List(Map("role" -> "user", "content" -> ctx.query.toString)))

      env = AgentLoopEnv(node, systemPrompt, toolDefs, ctxWithSchema, agentEventId)
      (finalResult, iterations) <- runAgentLoop(env, messages)

      durationMs = java.lang.System.currentTimeMillis() - t0
      _ <- ZIO.succeed(emit(ctxWithSchema, EventType.AgentEnd, node.nodeId, node.name, Map(
        "text_summary" -> summarize(finalResult),
        "full_response" -> finalResult.get("text").map(_.toString).getOrElse(""),
        "intent" -> finalResult.get("intent").map(_.toString).getOrElse("default"),
        "iterations" -> iterations
      ), parentEventId = Some(agentEventId), durationMs = Some(durationMs)))

    } yield finalResult
  }

  private def emitAgentStart(node: AgentNode, composed: ComposedAgent, systemPrompt: String, ctx: ExecutionContext, agentEventId: String): Unit = {
    emit(ctx, EventType.AgentStart, node.nodeId, node.name, Map(
      "query" -> ctx.query,
      "system" -> systemPrompt,
      "model" -> node.model,
      "tools" -> composed.tools.map(_.name),
      "context" -> composed.context.map(_.name).filter(_.nonEmpty),
      "context_scores" -> composed.contextSelection.map(s => Map(
        "name" -> s.context.name,
        "score" -> s.score,
        "source" -> s.source,
        "hops" -> s.hops
      ))
    ))
    if (composed.context.nonEmpty) emit(ctx, EventType.ContextInject, node.nodeId, node.name, Map(
      "context_names" -> composed.context.map(_.name),
      "count" -> composed.context.length,
      "selected_contexts" -> composed.contextSelection.map(s => Map(
        "node_id" -> s.context.nodeId,
        "name" -> s.context.name,
        "score" -> s.score,
        "source" -> s.source,
        "hops" -> s.hops,
        "token_count" -> s.tokenCount,
        "path" -> s.path,
        "reasons" -> s.reasons
      ))
    ), parentEventId = Some(agentEventId))
  }

  /** Everything an agent turn needs that doesn't change across turns of one run. */
  private case class AgentLoopEnv(
    node: AgentNode,
    systemPrompt: String,
    toolDefs: List[Map[String, Any]],
    ctx: ExecutionContext,
    parentEventId: String
  )

  /** How many times each bounded recovery has already fired. Kept in one value
    * so the turn signature stays readable as recoveries are added. */
  private case class Turn(
    messages: List[Map[String, Any]],
    iterations: Int,
    maxTokens: Int,
    tokenEscalations: Int,
    lastResponse: Option[LLMResponse],
    guards: LoopGuards
  )

  private def runAgentLoop(env: AgentLoopEnv, initialMessages: List[Map[String, Any]]): ZIO[Any, Throwable, (Map[String, Any], Int)] =
    agentTurn(env, Turn(initialMessages, 0, env.node.maxTokens, 0, None, LoopGuards()))

  private def agentTurn(env: AgentLoopEnv, turn: Turn): ZIO[Any, Throwable, (Map[String, Any], Int)] =
    if (turn.iterations >= env.node.maxIterations) exhaustedTurn(env, turn)
    else
      callWithRetry(
        () => backend.chat(env.node.model, env.systemPrompt, turn.messages, env.toolDefs, turn.maxTokens),
        env.ctx,
        env.node,
        env.node.executionPolicy
      ).flatMap { response =>
        val next = turn.iterations + 1
        // Stat'd once per turn, and Nil without touching the filesystem unless the
        // node actually declares artefacts that define "done".
        val missingPaths = GraphExecutor.missingCompletionPaths(env.node)
        // Turn kinds in priority order; the first applicable one wins.
        val handlers = List(
          toolTurn(env, response, turn, next),
          toolCallCorrection(env, response, turn, next),
          tokenEscalation(env, response, turn, next),
          narrationNudge(env, response, turn, next),
          completionNudge(env, response, turn, next, missingPaths)
        )
        handlers.flatten.headOption.getOrElse(finalAnswer(env, response, next))
      }

  /** The budget ran out: salvage the best text seen, else fall back to the last
    * message (with zero turns that is the user query, mirroring upstream). */
  private def exhaustedTurn(env: AgentLoopEnv, turn: Turn): ZIO[Any, Throwable, (Map[String, Any], Int)] = {
    val fallback = turn.lastResponse.map(r => if (r.text.nonEmpty) r.text else r.reasoning)
      .filter(_.nonEmpty)
      .getOrElse(turn.messages.last.get("content").map(_.toString).getOrElse(""))
    ZIO.succeed((Map("text" -> fallback, "intent" -> "default"), turn.iterations))
  }

  /** The model wants tools: run them, extend the conversation, and go around. */
  private def toolTurn(env: AgentLoopEnv, response: LLMResponse, turn: Turn, next: Int): Option[ZIO[Any, Throwable, (Map[String, Any], Int)]] =
    Option.when(response.toolCalls.nonEmpty) {
      handleToolCalls(response, env.node, env.ctx, Some(env.parentEventId)).flatMap { toolResults =>
        val extended = backend.extendMessages(turn.messages, response, toolResults)
        val compacted =
          if (GraphExecutor.HistoryCompactEvery > 0 && next % GraphExecutor.HistoryCompactEvery == 0)
            GraphExecutor.compactHistory(extended)
          else extended
        agentTurn(env, turn.copy(messages = compacted, iterations = next, lastResponse = Some(response)))
      }
    }

  /** Shared shape of the three re-prompt recoveries: extend with a user message,
    * emit a bounded Retry event, and go around with the guard bumped. */
  private def nudgeTurn(
    env: AgentLoopEnv,
    response: LLMResponse,
    turn: Turn,
    next: Int,
    payload: Map[String, Any],
    prompt: String,
    bump: LoopGuards => LoopGuards
  ): ZIO[Any, Throwable, (Map[String, Any], Int)] = {
    val nudged = backend.extendMessages(turn.messages, response, Nil) :+
      Map[String, Any]("role" -> "user", "content" -> prompt)
    ZIO.succeed(emit(env.ctx, EventType.Retry, env.node.nodeId, env.node.name, payload, parentEventId = Some(env.parentEventId))) *>
      agentTurn(env, turn.copy(messages = nudged, iterations = next, lastResponse = Some(response), guards = bump(turn.guards)))
  }

  /** The model wrote a tool call as TEXT and the API layer never parsed it, so
    * no tool ran — the turn only LOOKS final. Tell it the call wasn't executed
    * and let it re-issue a well-formed one, instead of accepting raw markup as
    * the agent's result (which ended steps mid-task on local models). */
  private def toolCallCorrection(env: AgentLoopEnv, response: LLMResponse, turn: Turn, next: Int): Option[ZIO[Any, Throwable, (Map[String, Any], Int)]] =
    Option.when(
      response.stopReason == "unparsed_tool_call" &&
      turn.guards.toolCallCorrections < GraphExecutor.MaxToolCallCorrections
    ) {
      nudgeTurn(env, response, turn, next,
        Map("reason" -> "unparsed_tool_call", "correction" -> (turn.guards.toolCallCorrections + 1), "text" -> response.text.take(400)),
        GraphExecutor.ToolCallCorrectionPrompt,
        g => g.copy(toolCallCorrections = g.toolCallCorrections + 1))
    }

  /** A truncated turn: escalate max_tokens (bounded) instead of accepting a
    * half-answer. The conversation itself is unchanged. */
  private def tokenEscalation(env: AgentLoopEnv, response: LLMResponse, turn: Turn, next: Int): Option[ZIO[Any, Throwable, (Map[String, Any], Int)]] =
    Option.when(
      response.stopReason == "max_tokens" &&
      turn.tokenEscalations < GraphExecutor.MaxTokenEscalations &&
      GraphExecutor.TokenEscalationFactor > 1 &&
      turn.maxTokens < GraphExecutor.TokenEscalationCap
    ) {
      val escalatedTokens = math.min(
        turn.maxTokens * GraphExecutor.TokenEscalationFactor,
        GraphExecutor.TokenEscalationCap
      )
      ZIO.succeed(emit(env.ctx, EventType.Retry, env.node.nodeId, env.node.name, Map(
        "reason" -> "max_tokens_truncation",
        "escalation" -> (turn.tokenEscalations + 1),
        "max_tokens" -> escalatedTokens
      ), parentEventId = Some(env.parentEventId))) *>
        agentTurn(env, turn.copy(iterations = next, maxTokens = escalatedTokens,
          tokenEscalations = turn.tokenEscalations + 1, lastResponse = Some(response)))
    }

  /** A blank or announce-and-stop turn is not an answer — the model was about
    * to act. Nudge it to continue (bounded) instead of accepting a non-answer
    * as the agent's result and ending the step mid-task. */
  private def narrationNudge(env: AgentLoopEnv, response: LLMResponse, turn: Turn, next: Int): Option[ZIO[Any, Throwable, (Map[String, Any], Int)]] =
    Option.when(
      turn.guards.continuationNudges < GraphExecutor.MaxContinuationNudges &&
      GraphExecutor.looksLikeNarrationOnly(response.text, response.reasoning)
    ) {
      nudgeTurn(env, response, turn, next,
        Map("reason" -> "narration_only_turn", "nudge" -> (turn.guards.continuationNudges + 1), "text" -> response.text.take(200)),
        GraphExecutor.ContinuationNudge,
        g => g.copy(continuationNudges = g.continuationNudges + 1))
    }

  /** The node declares artefacts that define "done" and some are missing, so
    * the work is not finished no matter how the closing message reads. Say
    * which ones and let it finish (bounded) rather than accepting the claim. */
  private def completionNudge(env: AgentLoopEnv, response: LLMResponse, turn: Turn, next: Int, missingPaths: List[String]): Option[ZIO[Any, Throwable, (Map[String, Any], Int)]] =
    Option.when(turn.guards.completionNudges < GraphExecutor.MaxCompletionNudges && missingPaths.nonEmpty) {
      nudgeTurn(env, response, turn, next,
        Map("reason" -> "missing_completion_paths", "nudge" -> (turn.guards.completionNudges + 1), "missing" -> missingPaths.take(10)),
        GraphExecutor.completionNudge(missingPaths),
        g => g.copy(completionNudges = g.completionNudges + 1))
    }

  private def finalAnswer(env: AgentLoopEnv, response: LLMResponse, next: Int): ZIO[Any, Throwable, (Map[String, Any], Int)] = {
    val finalText = if (response.text.nonEmpty) response.text else response.reasoning
    ZIO.succeed((Map[String, Any]("text" -> finalText, "intent" -> inferIntent(finalText, env.node)), next))
  }

  private def executeTool(node: ToolNode, ctx: ExecutionContext, inputData: Option[Map[String, Any]] = None): ZIO[Any, Throwable, Any] = {
    val fn = toolFns.get(node.callableRef)
    if (fn.isEmpty) {
      ZIO.fail(new RuntimeException(s"Tool callable not registered: ${node.callableRef}"))
    } else {
      // An empty map is a VALID call: a tool whose parameters are all optional is
      // invoked with no arguments. `inputData` being an Option already keeps that
      // distinct from a genuinely absent input, which alone falls back to the
      // upstream payload. An empty map must not be treated as absent either:
      // that would silently inherit the previous node's output.
      val payload = inputData.getOrElse(ctx.outputs.values.lastOption.map(_.asInstanceOf[Map[String, Any]]).getOrElse(Map.empty))
      // Nudge argument TYPES to what the tool declares before validating: a local
      // model's comma-separated string for an array param is unambiguous, and losing
      // the whole turn to `tool_input.x must be an array` throws away work it did
      // correctly. Only schema-declared, lossless conversions — see coerceToSchema.
      val runtimePayload = GraphExecutor.coerceToSchema(payload.asInstanceOf[Map[String, Any]], node.inputSchema)
      
      for {
        result <- callWithRetry(
          () => ZIO.succeed(fn.get.apply(runtimePayload)),
          ctx,
          node,
          node.executionPolicy
        )
      } yield result
    }
  }

  private def executeTransform(node: TransformNode, ctx: ExecutionContext): ZIO[Any, Throwable, Any] = {
    val fn = toolFns.get(node.callableRef)
    if (fn.isEmpty) {
      ZIO.fail(new RuntimeException(s"Transform callable not registered: ${node.callableRef}"))
    } else {
      // Collect fan-in inputs: try each inputKey as a node_id in ctx.outputs,
      // then as a key in ctx.state.data.  Fall back to last output or full
      // state.data when no inputKeys are declared.
      val inputData: Map[String, Any] =
        if (node.inputKeys.nonEmpty) {
          node.inputKeys.foldLeft(Map.empty[String, Any]) { (acc, key) =>
            if (ctx.outputs.contains(key)) acc + (key -> ctx.outputs(key))
            else if (ctx.state.data.contains(key)) acc + (key -> ctx.state.data(key))
            else acc
          }
        } else if (ctx.outputs.nonEmpty) {
          val last = ctx.outputs.values.last
          last match {
            case m: Map[?, ?] => m.asInstanceOf[Map[String, Any]]
            case v            => Map("input" -> v)
          }
        } else {
          ctx.state.data
        }

      val t0 = java.lang.System.currentTimeMillis()

      for {
        result <- callWithRetry(
          () => ZIO.succeed(fn.get.apply(inputData)),
          ctx,
          node,
          node.executionPolicy
        )
        durationMs = java.lang.System.currentTimeMillis() - t0
        _ <- ZIO.succeed(emit(ctx, EventType.ToolResult, node.nodeId, node.name, Map(
          "callable_ref" -> node.callableRef,
          "output_summary" -> summarize(result),
          "success" -> true,
          "duration_ms" -> durationMs
        )))
      } yield result
    }
  }

  private def executeReasoner(node: ReasonerNode, ctx: ExecutionContext, parentEventId: Option[String] = None): ZIO[Any, Throwable, Map[String, Any]] = {
    import loki.engine.symbolic._
    for {
      facts   <- gatherReasonerFacts(node, ctx)
      solved  <- ZIO.attempt {
        val compiledRules: List[Rule] =
          (if (node.program.nonEmpty) parseProgram(node.program) else Nil) ++
          node.rules.flatMap(m => ruleFromObj(m.asInstanceOf[Any]))
        val program  = new Program(compiledRules)
        val solution = program.solve(facts, withProof = node.withProof)
        val emitted  = if (node.emitDerivedOnly) solution.derived else solution.facts
        val filtered = if (node.query.nonEmpty) {
          val wanted = node.query.toSet
          emitted.filter(f => f.headOption.exists(h => wanted.contains(h.toString)))
        } else emitted
        (filtered, solution)
      }
      _ <- if (node.failOnEmpty && solved._1.isEmpty)
        ZIO.fail(new RuntimeException(
          s"ReasonerNode '${node.name}' derived no facts for query ${node.query.mkString(",")}"
        ))
      else ZIO.unit
      result = {
        val (filtered, solution) = solved
        val base: Map[String, Any] = Map(
          "facts"       -> filtered.toList.sortBy(_.toString).map(factToDict),
          "fact_count"  -> filtered.size,
          "input_count" -> facts.size,
          "predicates"  -> filtered.flatMap(_.headOption).map(_.toString).toList.distinct.sorted
        )
        val withProofs =
          if (node.withProof)
            base + ("proofs" -> filtered.toList.sortBy(_.toString).collect {
              case f if solution.justifications.contains(f) =>
                Map("fact" -> factToDict(f), "explanation" -> solution.explain(f))
            })
          else base
        withProofs
      }
      _ <- ZIO.succeed(emit(ctx, EventType.ToolResult, node.nodeId, node.name, Map(
        "tool_name"      -> node.name,
        "node_type"      -> node.nodeType.toString,
        "output_summary" -> summarize(result),
        "fact_count"     -> result.getOrElse("fact_count", 0),
        "success"        -> true
      ), parentEventId))
    } yield result
  }

  private def gatherReasonerFacts(node: ReasonerNode, ctx: ExecutionContext): ZIO[Any, Throwable, List[Any]] = {
    val src = node.factSource
    ZIO.attempt {
      val facts = scala.collection.mutable.ListBuffer[Any]()

      if (src.stateKeys.nonEmpty) {
        for (key <- src.stateKeys) {
          ctx.state.data.get(key) match {
            case Some(list: List[_]) => facts ++= list
            case Some(v)             => facts += v
            case None                =>
          }
        }
      } else {
        if (ctx.outputs.nonEmpty) {
          val last = ctx.outputs.values.last
          last match {
            case list: List[_]                           => facts ++= list
            case m: Map[_, _] if m.asInstanceOf[Map[String,Any]].get("facts").isInstanceOf[Some[List[_]]] =>
              facts ++= m.asInstanceOf[Map[String,Any]]("facts").asInstanceOf[List[_]]
            case _ =>
          }
        }
        ctx.state.data.get("facts") match {
          case Some(list: List[_]) => facts ++= list
          case _                   =>
        }
      }
      facts.toList
    }.flatMap { baseFacts =>
      if (src.edgeTypes.isEmpty && !src.includeNodeFacts)
        ZIO.succeed(baseFacts)
      else {
        for {
          kgFacts <- ZIO.foreach(src.edgeTypes) { etype =>
            val edgeTypeOpt = EdgeType.values.find(_.value == etype)
            val listZ: ZIO[Any, Throwable, List[Edge]] = edgeTypeOpt match {
              case Some(et) => store.listEdges(Some(et))
              case None     => store.listEdges().map(_.filter(e => e.edgeType.value == etype))
            }
            listZ.flatMap { edges =>
              val pred = etype.toLowerCase
              ZIO.foreach(edges) { e =>
                for {
                  srcLabelOpt <- if (src.useNodeNames) store.getNode(e.srcId).map(_.map(_.name).filter(_.nonEmpty).getOrElse(e.srcId)) else ZIO.succeed(e.srcId)
                  dstLabelOpt <- if (src.useNodeNames) store.getNode(e.dstId).map(_.map(_.name).filter(_.nonEmpty).getOrElse(e.dstId)) else ZIO.succeed(e.dstId)
                } yield List(pred, srcLabelOpt, dstLabelOpt): Any
              }
            }
          }.map(_.flatten)
          nodeFacts <- if (src.includeNodeFacts)
            store.listNodes().map(_.map { n =>
              val label = if (src.useNodeNames && n.name.nonEmpty) n.name else n.nodeId
              List(n.nodeType.toString, label): Any
            })
          else ZIO.succeed(List.empty[Any])
        } yield baseFacts ++ kgFacts ++ nodeFacts
      }
    }
  }

  def inspectResume(
    ctx:                 ExecutionContext,
    staleAfterSeconds:   Double = 1200.0,
    requiredOutputs:     List[String] = Nil,
    now:                 Option[java.time.Instant] = None
  ): ResumeReadiness = {
    val lastEventAt = ctx.trace.map(_.timestamp).maxByOption(_.getEpochSecond)
    val (isStale, secondsSince) = lastEventAt match {
      case None    => (false, None)
      case Some(t) =>
        val current = now.getOrElse(java.time.Instant.now())
        val secs    = (current.getEpochSecond - t.getEpochSecond).toDouble
        (secs > staleAfterSeconds, Some(secs))
    }
    val (available, unrecoverable) = requiredOutputs.partition { key =>
      ctx.outputs.contains(key) || ctx.state.data.contains(key)
    }
    ResumeReadiness(
      status                = ctx.state.status,
      isPaused              = ctx.isPaused,
      isStale               = isStale,
      lastEventAt           = lastEventAt,
      secondsSinceLastEvent = secondsSince,
      available             = available,
      unrecoverable         = unrecoverable
    )
  }

  def checkpointContext(
    ctx:  ExecutionContext,
    name: String = "Execution checkpoint"
  ): ZIO[Any, Throwable, ContextNode] = {
    val json = JsonUtil.toJson(ctx.snapshot)
    val checkpoint = ContextNode(
      name        = name,
      description = "Serialized execution checkpoint",
      content     = json,
      contentType = "json",
      source      = "checkpoint",
      groupId     = ctx.sessionId,
      attributes  = Map(
        "origin"        -> "checkpoint",
        "session_id"    -> ctx.sessionId,
        "graph_version" -> ctx.state.graphVersion
      )
    )
    for {
      _ <- store.upsertNode(checkpoint)
      _ <- ZIO.succeed(emit(ctx, EventType.Checkpoint, checkpoint.nodeId, checkpoint.name,
                            Map("checkpoint_node_id" -> checkpoint.nodeId)))
    } yield checkpoint
  }

  def loadCheckpoint(checkpointNodeId: String): ZIO[Any, Throwable, ExecutionContext] =
    store.getNode(checkpointNodeId).flatMap {
      case Some(n: ContextNode) =>
        ZIO.attempt {
          val data = JsonUtil.fromJson(n.content).asInstanceOf[Map[String, Any]]
          ExecutionContext.fromSnapshot(data)
        }
      case _ =>
        ZIO.fail(new RuntimeException(s"Checkpoint node not found: $checkpointNodeId"))
    }

  def resumeFromCheckpoint(
    checkpointNodeId: String,
    entryNodeId:      String,
    query:            Option[Any] = None,
    strategy:         String = "sequential"
  ): ZIO[Any, Throwable, ExecutionContext] =
    loadCheckpoint(checkpointNodeId).flatMap { ctx =>
      resume(entryNodeId, ctx, query, strategy)
    }

  def materialiseOutput(sourceNode: AnyNode, output: Any, ctx: ExecutionContext): ZIO[Any, Throwable, ContextNode] = {
    val content = if (output.isInstanceOf[String]) output.asInstanceOf[String] else output.toString
    val ctxNode = ContextNode(
      name        = s"Output of ${sourceNode.name}",
      description = s"Auto-materialised output from ${sourceNode.nodeType} node",
      content     = content,
      source      = s"node:${sourceNode.nodeId}",
      groupId     = ctx.sessionId,
      attributes  = Map(
        "origin"         -> "runtime",
        "session_id"     -> ctx.sessionId,
        "source_node_id" -> sourceNode.nodeId
      )
    )
    for {
      _ <- store.upsertNode(ctxNode)
      _ <- store.upsertEdge(Edge.produces(sourceNode.nodeId, ctxNode.nodeId))
    } yield ctxNode
  }

  private def executeApproval(node: ApprovalNode, ctx: ExecutionContext): ZIO[Any, Throwable, Map[String, Any]] = {
    val payload = ctx.state.data.get(node.inputKey).map(_.asInstanceOf[Map[String, Any]]).getOrElse(Map.empty)
    
    if (payload.contains("approved")) {
      val approved = payload.get("approved").map(_.toString.toBoolean).getOrElse(false)
      val assignedTo = payload.get("assigned_to").map(_.toString)
      val task = ctx.state.inbox.values.find(t => t.nodeId == node.nodeId && t.status == "pending")
      task.foreach { t =>
        ctx.state.inbox.updated(t.taskId, t.copy(status = if (approved) "approved" else "rejected", assignedTo = assignedTo.orElse(t.assignedTo), resolvedAt = Some(Instant.now())))
      }
      ZIO.succeed(Map(
        "approved" -> approved,
        "next_node_id" -> (if (approved) node.approvedTargetId else node.rejectedTargetId),
        "assigned_to" -> assignedTo.getOrElse("")
      ))
    } else {
      val assignedTo = if (node.requireAssignment && node.assignees.nonEmpty) Some(node.assignees.head) else None
      val task = ApprovalTask(
        taskId = UUID.randomUUID().toString,
        nodeId = node.nodeId,
        token = UUID.randomUUID().toString,
        assignees = node.assignees,
        assignedTo = assignedTo,
        waitingFor = Some(if (node.instructions.nonEmpty) node.instructions else node.name),
        dueAt = node.slaSeconds.map(s => Instant.now().plusSeconds(s)),
        escalationTarget = if (node.escalationTarget.nonEmpty) Some(node.escalationTarget) else None,
        metadata = Map("assignment_mode" -> node.assignmentMode, "input_key" -> node.inputKey)
      )
      val newCtx = ctx.copy(state = ctx.state.copy(
        inbox = ctx.state.inbox + (task.taskId -> task),
        status = "paused",
        pendingPause = Some(WorkflowPause(
          reason = "approval_wait",
          nodeId = node.nodeId,
          nodeName = node.name,
        waitingFor = Some(if (node.instructions.nonEmpty) node.instructions else node.name),
          metadata = Map("task_id" -> task.taskId, "token" -> task.token)
        ))
      ))
      pendingPause.set(newCtx.state.pendingPause)
      ZIO.succeed(Map("status" -> "paused", "task_id" -> task.taskId, "next_node_id" -> None))
    }
  }

  private def handleToolCalls(response: LLMResponse, agentNode: AgentNode, ctx: ExecutionContext, parentEventId: Option[String]): ZIO[Any, Throwable, List[ToolResult]] = {
    for {
      toolEdges <- store.getEdges(agentNode.nodeId, Some(EdgeType.HAS_TOOL))
      toolMap <- ZIO.foreach(toolEdges) { edge =>
        store.getNode(edge.dstId).map {
          _.collect { case n: ToolNode => n }.map(n => n.name -> n)
        }
      }.map(_.flatten.toMap)

      results <- ZIO.foreach(response.toolCalls) { tc =>
        val toolNode = toolMap.get(tc.name)
        val callableRef = toolNode.map(_.callableRef).getOrElse("")

        val callEvent = emit(ctx, EventType.ToolCall, toolNode.map(_.nodeId).getOrElse(agentNode.nodeId), tc.name, Map(
          "tool_name" -> tc.name,
          "callable_ref" -> callableRef,
          "input" -> tc.input
        ), parentEventId = parentEventId)

        val t0 = java.lang.System.currentTimeMillis()
        val executed: UIO[(String, Boolean)] = toolNode match {
          // Name what IS on the belt. "Tool 'read_file' not found." is a dead end: the
          // model has no way to learn the belt from the error, so it re-tries the same
          // absent tool or gives up, burning turns either way. A leashed worker with
          // only {read_input, write_file} routinely reaches for read_file. Listing the
          // alternatives lets it self-correct on the next turn, which is the difference
          // between a wasted turn and a recovered one. Sorted for determinism.
          //
          // Deliberately NO "did you mean X?" guess: a purely lexical match is blind to
          // intent and steers confidently wrong — read_file scores closer to write_file
          // than to read_input (they share the longer `_file`), so a worker trying to
          // READ would be pointed at the writer. Listing the belt lets the model pick
          // using the intent only it has.
          //
          // Before this the branch did not exist at all: `toolNode.get` threw, and the
          // model was handed a bare NoSuchElementException.
          case None =>
            val available = toolMap.keys.filter(_.nonEmpty).toList.sorted
            val message =
              if (available.nonEmpty) s"Tool '${tc.name}' not found. Available tools: ${available.mkString(", ")}."
              else s"Tool '${tc.name}' not found. This node has no tools — answer directly instead of calling a tool."
            ZIO.succeed((message, false))
          case Some(resolved) =>
            executeTool(resolved, ctx, Some(tc.input)).foldCauseZIO(
              cause => ZIO.succeed((s"Tool error: ${cause.squash}", false)),
              raw => ZIO.succeed((if (raw.isInstanceOf[String]) raw.asInstanceOf[String] else String.valueOf(raw), true))
            )
        }

        executed.map { case (content, success) =>
          val durationMs = java.lang.System.currentTimeMillis() - t0
          emit(ctx, EventType.ToolResult, toolNode.map(_.nodeId).getOrElse(agentNode.nodeId), tc.name, Map(
            "tool_name" -> tc.name,
            "output_summary" -> summarize(content),
            "success" -> success
          ), parentEventId = Some(callEvent.eventId), durationMs = Some(durationMs))
          ToolResult(tc.id, content)
        }
      }
    } yield results
  }

  private def routeNode(node: AgentNode, result: Any, ctx: ExecutionContext, parentEventId: Option[String]): ZIO[Any, Throwable, (String, TraceEvent)] = {
    val resultMap = result.asInstanceOf[Map[String, Any]]
    
    val routeResult: Option[(String, TraceEvent)] = node.decisionTable match {
      case Some(dt) =>
        val decision = evaluateDecisionTable(dt, ctx.state, resultMap)
        decision.map { d =>
          (if (d.targetNodeId == "__END__") "" else d.targetNodeId, emit(ctx, EventType.Routing, node.nodeId, node.name, Map(
            "intent" -> d.name.orElse("decision_table"),
            "next_node_id" -> (if (d.targetNodeId == "__END__") None else Some(d.targetNodeId)),
            "confidence" -> 1.0,
            "mode" -> "decision_table"
          ), parentEventId = parentEventId))
        }.orElse {
          if (dt.strict) {
            Some((if (dt.defaultTargetId == "__END__") "" else dt.defaultTargetId, emit(ctx, EventType.Routing, node.nodeId, node.name, Map(
              "intent" -> dt.defaultIntent,
              "next_node_id" -> (if (dt.defaultTargetId == "__END__") None else Some(dt.defaultTargetId)),
              "confidence" -> 1.0,
              "mode" -> "decision_table"
            ), parentEventId = parentEventId)))
          } else None
        }
      case None => None
    }
    
    routeResult.orElse {
      node.routeRules.sortBy(-_.priority).find { rule =>
        evaluateRouteRule(rule, resultMap, ctx.state)
      }.map { rule =>
        (if (rule.targetNodeId == "__END__") "" else rule.targetNodeId, emit(ctx, EventType.Routing, node.nodeId, node.name, Map(
          "intent" -> rule.name,
          "next_node_id" -> (if (rule.targetNodeId == "__END__") None else Some(rule.targetNodeId)),
          "confidence" -> 1.0,
          "mode" -> "deterministic"
        ), parentEventId = parentEventId))
      }
    }.getOrElse {
      val intent = resultMap.get("intent").map(_.toString).getOrElse("default")
      val nextId = node.routingTable.get(intent).orElse(node.routingTable.get("default")).getOrElse("__END__")
      (nextId, emit(ctx, EventType.Routing, node.nodeId, node.name, Map(
        "intent" -> intent,
        "next_node_id" -> (if (nextId == "__END__") None else Some(nextId)),
        "confidence" -> None,
        "mode" -> "llm"
      ), parentEventId = parentEventId))
    } match {
      case (id, event) => ZIO.succeed((id, event))
    }
  }

  private def evaluateDecisionTable(dt: DecisionTable, state: WorkflowState, result: Map[String, Any]): Option[DecisionRule] = {
    dt.rules.sortBy(-_.priority).find { rule =>
      rule.conditions.forall { cond =>
        evaluateConstraintRule(cond, state, result)
      }
    }
  }

  private def evaluateConstraintRule(rule: ConstraintRule, state: WorkflowState, result: Any): Boolean = {
    val sourceData = rule.source match {
      case "state" => state.data
      case _ => result.asInstanceOf[Map[String, Any]]
    }
    val left = if (rule.path.nonEmpty) resolveJsonPath(sourceData, rule.path) else sourceData
    val right = rule.value
    compareValues(rule.operator, left, right)
  }

  private def evaluateRouteRule(rule: RouteRule, result: Map[String, Any], state: WorkflowState): Boolean = {
    val sourceData = if (rule.source == "state") state.data else result
    val left = if (rule.path.nonEmpty) resolveJsonPath(sourceData, rule.path) else sourceData
    compareValues(rule.operator, left, rule.value)
  }

  private def resolveJsonPath(data: Map[String, Any], path: String): Any = {
    path.split("\\.").filter(_.nonEmpty).foldLeft[Any](data) { (current, part) =>
      current match {
        case m: Map[_, _] => m.asInstanceOf[Map[String, Any]].get(part)
        case _ => None
      }
    }
  }

  private def unboxNumber(a: Any): Double = a match {
    case n: Number => n.doubleValue
    case s: String => s.toDoubleOption.getOrElse(0.0)
    case _ => 0.0
  }

  private def compareValues(operator: String, left: Any, right: Any): Boolean = {
    operator match {
      case "exists" => left != null
      case "truthy" => Boolean.unbox(left.asInstanceOf[Object])
      case "contains" => left != null && left.toString.contains(right.toString)
      case "not_equals" => left != right
      case "in" => right match {
        case l: List[_] => l.contains(left)
        case _ => false
      }
      case "regex" => left match {
        case s: String => s.matches(right.toString)
        case _ => false
      }
      case "gt" => unboxNumber(left) > unboxNumber(right)
      case "gte" => unboxNumber(left) >= unboxNumber(right)
      case "lt" => unboxNumber(left) < unboxNumber(right)
      case "lte" => unboxNumber(left) <= unboxNumber(right)
      case _ => left == right
    }
  }

  private def inferIntent(text: String, node: AgentNode): String =
    node.routingTable.keys.filter(_ != "default").toList match {
      case Nil      => "default"
      case intents  => intents.find(intent => intent.toLowerCase.r.findAllIn(text.toLowerCase).nonEmpty).getOrElse("default")
    }

  private def callWithRetry[T](fn: () => ZIO[Any, Throwable, T], ctx: ExecutionContext, node: AnyNode, policy: ExecutionPolicy): ZIO[Any, Throwable, T] = {
    def loop(attempt: Int, delay: Double): ZIO[Any, Throwable, T] = {
      fn().catchSome { case e =>
        if (attempt < policy.retryPolicy.maxAttempts) {
          val retryEvent = emit(ctx, EventType.Retry, node.nodeId, node.name, Map(
            "attempt" -> attempt,
            "max_attempts" -> policy.retryPolicy.maxAttempts,
            "error" -> e.getMessage
          ))
          if (delay > 0) {
            ZIO.sleep(java.time.Duration.ofMillis((delay * 1000).toLong)).flatMap { _ =>
              loop(attempt + 1, delay * policy.retryPolicy.backoffMultiplier)
            }
          } else {
            loop(attempt + 1, delay)
          }
        } else {
          ZIO.fail(e)
        }
      }
    }
    loop(1, policy.retryPolicy.backoffSeconds)
  }

  def route(query: String, candidates: Option[List[AgentNode]] = None): ZIO[Any, Throwable, RoutingDecision] = {
    for {
      allNodes <- store.listNodes(Some(NodeType.Agent))
      agentNodes = allNodes.collect { case a: AgentNode if a.isValid => a }
      finalCandidates = candidates.getOrElse(agentNodes)
      
      decision <- if (finalCandidates.isEmpty) {
        ZIO.fail(new IllegalArgumentException("No valid AgentNode candidates found"))
      } else if (finalCandidates.length == 1) {
        ZIO.succeed(RoutingDecision(finalCandidates.head.nodeId, "Only one agent available.", 1.0))
      } else {
        val agentList = finalCandidates.map(n => s"- ${n.nodeId}: ${n.description}").mkString("\n")
        val prompt = s"""Select the best agent for this task.

Available agents:
$agentList

Task: $query

Respond with JSON only:
{{"agent": "<id>", "reason": "<one sentence explaining why>", "confidence": <0.0-1.0>}}"""
        
        backend.chat(routerModel, "You are a routing classifier.", List(Map("role" -> "user", "content" -> prompt)), Nil)
          .map { response =>
            try {
              val data = parseJson(response.text)
              RoutingDecision(
                data("agent").toString,
                data.get("reason").map(_.toString).getOrElse(""),
                data.get("confidence").map(_.toString.toDouble).getOrElse(0.5)
              )
            } catch {
              case _: Exception => RoutingDecision(finalCandidates.head.nodeId, "Fallback: router response could not be parsed.", 0.5)
            }
          }
      }
    } yield decision
  }

  def execute(agentId: String, query: Any, routing: Option[RoutingDecision] = None): ZIO[Any, Throwable, AgentResult] = {
    for {
      nodeOpt <- store.getNode(agentId)
      node <- nodeOpt match {
        case Some(a: AgentNode) => ZIO.succeed(a)
        case _ => ZIO.fail(new IllegalArgumentException(s"Agent node not found: $agentId"))
      }
      composed <- composer.compose(node, Some(queryText(query)))
      contextNames = composed.context.map(_.name).filter(_.nonEmpty)
      ctx = ExecutionContext(query = query)
      raw <- executeAgent(node, ctx)
      text = raw.asInstanceOf[Map[String, Any]].get("text").map(_.toString).getOrElse("")
      decision = routing.getOrElse(RoutingDecision(agentId, "Direct execution.", 1.0))
    } yield AgentResult(
      routedTo = agentId,
      reason = decision.reason,
      confidence = decision.confidence,
      contextInjected = contextNames,
      result = text,
      lowConfidenceWarning = decision.lowConfidenceWarning
    )
  }

  private def queryText(query: Any): String = query match {
    case s: String => s
    case l: List[_] => l.collect { case m: Map[String, _] if m.get("type") == Some("text") => m.get("text").map(_.toString).getOrElse("") }.mkString(" ")
    case _ => query.toString
  }

  private def textOf(value: Any): String = value match {
    case s: String => s
    case m: Map[_, _] =>
      m.asInstanceOf[Map[String, Any]].get("text").orElse(m.asInstanceOf[Map[String, Any]].get("content")).map(_.toString).getOrElse(value.toString)
    case l: List[_] => l.map(textOf).mkString("\n")
    case _ => if (value != null) value.toString else ""
  }

  private def summarize(value: Any, maxLen: Int = 200): String = {
    val s = if (value != null) value.toString else ""
    if (s.length > maxLen) s.take(maxLen) + "…" else s
  }

  private def parseJson(text: String): Map[String, Any] = {
    val cleaned = text.trim
    val start = cleaned.indexOf('{')
    val end = cleaned.lastIndexOf('}')
    if (start < 0 || end <= start) Map.empty
    else {
      val jsonStr = cleaned.substring(start, end + 1)
      val strPat = """"(\w+)"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
      val numPat = """"(\w+)"\s*:\s*([\d.]+)""".r
      val boolPat = """"(\w+)"\s*:\s*(true|false)""".r
      val fromStrings = strPat.findAllMatchIn(jsonStr).map(m => m.group(1) -> m.group(2)).toMap
      val fromNumbers = numPat.findAllMatchIn(jsonStr).map(m => m.group(1) -> m.group(2).toDoubleOption.getOrElse(0.0)).toMap
      val fromBooleans = boolPat.findAllMatchIn(jsonStr).map(m => m.group(1) -> (m.group(2) == "true")).toMap
      (fromBooleans ++ fromNumbers ++ fromStrings)
    }
  }

  private def parseJsonArray(text: String): List[Map[String, Any]] = {
    val start = text.indexOf('[')
    val end = text.lastIndexOf(']')
    if (start < 0 || end <= start) Nil
    else {
      val arrayStr = text.substring(start + 1, end)
      val (_, _, objectsRev) = arrayStr.zipWithIndex.foldLeft[(Int, Option[Int], List[String])]((0, Option.empty[Int], List.empty[String])) {
        case ((depth, objStart, acc), ('{', i)) => (depth + 1, if (depth == 0) Some(i) else objStart, acc)
        case ((depth, objStart, acc), ('}', i)) =>
          if (depth - 1 == 0 && objStart.isDefined)
            (depth - 1, None, arrayStr.substring(objStart.get, i + 1) :: acc)
          else (depth - 1, objStart, acc)
        case (state, _) => state
      }
      objectsRev.reverse.map(o => parseJson(s"{$o}"))
    }
  }

  def plan(query: String, candidates: Option[List[AgentNode]] = None): ZIO[Any, Throwable, List[RoutingDecision]] = {
    for {
      allNodes <- store.listNodes(Some(NodeType.Agent))
      agentNodes = allNodes.collect { case a: AgentNode if a.isValid => a }
      finalCandidates = candidates.getOrElse(agentNodes)
      decisions <- if (finalCandidates.isEmpty) {
        ZIO.succeed(Nil)
      } else {
        val agentList = finalCandidates.map(n => s"- ${n.nodeId}: ${n.description}").mkString("\n")
        val prompt =
          s"""Plan a sequence of agents to handle this task.
Available agents:
$agentList
Task: $query
Respond with a JSON array only:
[{"agent": "<id>", "reason": "<one sentence>", "confidence": <0.0-1.0>}, ...]"""
        backend.chat(routerModel, "You are a planning classifier.", List(Map("role" -> "user", "content" -> prompt)), Nil)
          .map { response =>
            try {
              parseJsonArray(response.text).flatMap { item =>
                item.get("agent").map(_.toString).map { agentId =>
                  RoutingDecision(
                    agentId,
                    item.get("reason").map(_.toString).getOrElse(""),
                    item.get("confidence").map(_.asInstanceOf[Number].doubleValue).getOrElse(0.5)
                  )
                }
              }
            } catch { case _: Exception => Nil }
          }
      }
    } yield decisions
  }
}

case class ExecutionOptions(
  allowedTools: Option[Set[String]] = None
)

def printTrace(ctx: ExecutionContext, width: Int = 72): Unit = {
  val sep = "=" * width
  println()
  println(s"Session ${ctx.sessionId.take(8)}  '${queryText(ctx.query)}'")
  println(sep)
  
  val byId = ctx.trace.map(e => e.eventId -> e).toMap
  val children = ctx.trace.groupBy(_.parentEventId)
  
  def render(event: TraceEvent, indent: Int): Unit = {
    val pad = "  " * indent
    val t = event.eventType
    val p = event.payload
    
    t match {
      case EventType.Hop =>
        val nodeType = p.get("node_type").map(_.toString.split("\\.").last.toUpperCase).getOrElse("")
        val hopNum = p.get("hop").map(_.toString).getOrElse("")
        println(s"$pad HOP $hopNum  $nodeType  ${event.nodeName}")
        children.get(Some(event.eventId)).foreach(_.foreach(render(_, indent + 1)))
        
      case EventType.AgentStart =>
        val model = p.get("model").map(_.toString).getOrElse("?")
        val tools = p.get("tools").map(_.asInstanceOf[List[_]].mkString(", ")).getOrElse("none")
        println(s"$pad  tools: $tools")
        children.get(Some(event.eventId)).foreach(_.foreach(render(_, indent)))
        
      case EventType.ContextInject =>
        val names = p.get("context_names").map(_.asInstanceOf[List[_]].mkString(", ")).getOrElse("—")
        val count = p.get("count").map(_.toString).getOrElse("0")
        println(s"$pad  context_inject  $names  ($count nodes)")
        
      case EventType.AgentEnd =>
        val summary = p.get("text_summary").map(s => s"'$s'").getOrElse("")
        val intent = p.get("intent").map(_.toString).getOrElse("default")
        val ms = event.durationMs.map(m => s" [${m}ms]").getOrElse("")
        println(s"$pad  agent_end  $summary  intent=$intent$ms")
        
      case EventType.ToolCall =>
        val inp = p.get("input").map(_.toString.take(60)).getOrElse("{}")
        println(s"$pad  tool_call  ${event.nodeName}  $inp")
        children.get(Some(event.eventId)).foreach(_.foreach(render(_, indent + 1)))
        
      case EventType.ToolResult =>
        val success = p.get("success").map(_.toString).getOrElse("false")
        val status = if (success == "true") "ok" else "err"
        val summary = p.get("output_summary").map(s => s"'$s'").getOrElse("")
        val ms = event.durationMs.map(m => s" [${m}ms]").getOrElse("")
        println(s"$pad  tool_result  ${event.nodeName}  $status  $summary$ms")
        
      case EventType.Routing =>
        val intent = p.get("intent").map(_.toString).getOrElse("default")
        val nextId = p.get("next_node_id").map(_.toString).getOrElse("__END__")
        println(s"$pad  routing  $intent → $nextId")
        
      case EventType.SubgraphEnter =>
        val entry = p.get("entry_node_id").map(_.toString.take(8)).getOrElse("")
        println(s"$pad  subgraph_enter  ${event.nodeName}  entry=$entry")
        
      case EventType.SubgraphExit =>
        println(s"$pad  subgraph_exit   ${event.nodeName}")
        
      case _ =>
    }
  }
  
  children.get(None).foreach(_.foreach(render(_, 0)))
  
  val totalMs = ctx.trace.collect { case e if e.eventType == EventType.AgentEnd => e.durationMs.getOrElse(0L) }.sum
  val hops = ctx.trace.count(_.eventType == EventType.Hop)
  val ends = ctx.trace.count(_.eventType == EventType.AgentEnd)
  
  println(sep)
  val msStr = if (totalMs > 0) s" · ${totalMs}ms" else ""
  println(s"Total: $hops hops · $ends agent_end events$msStr\n")
}

private def queryText(query: Any): String = query match {
  case s: String => s
  case l: List[_] => l.collect { case m: Map[String, _] if m.get("type") == Some("text") => m.get("text").map(_.toString).getOrElse("") }.mkString(" ")
  case _ => if (query != null) query.toString else ""
}

def getRuntimeNodes(store: GraphStore, sessionId: Option[String] = None, onlyValid: Boolean = true): URIO[Any, List[ContextNode]] = {
  for {
    candidates <- store.listNodes(Some(NodeType.Context), sessionId, onlyValid)
  } yield candidates.collect { case c: ContextNode if c.attributes.get("origin") == Some("runtime") => c }
}

def cleanupSession(store: GraphStore, sessionId: String, hard: Boolean = false): URIO[Any, Int] = {
  for {
    nodes <- store.listNodes(groupId = Some(sessionId), onlyValid = false)
    count <- ZIO.foldLeft(nodes)(0) { (acc, node) =>
      for {
        _ <- store.getEdges(node.nodeId, Some(EdgeType.PRODUCES), "in", false).flatMap { edges =>
          ZIO.foreach(edges) { edge =>
            if (hard) store.deleteEdge(edge.edgeId) else store.expireEdge(edge.edgeId)
          }
        }
        _ <- if (hard) store.deleteNode(node.nodeId) else store.expireNode(node.nodeId)
      } yield acc + 1
    }
  } yield count
}
