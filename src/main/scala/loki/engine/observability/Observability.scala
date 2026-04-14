package loki.engine.observability

import zio.*
import loki.engine.core.executor.*
import loki.engine.core.nodes.*

case class RunHopExplanation(
  nodeId: String,
  nodeName: String,
  hop: Option[Int] = None,
  summary: String = ""
)

case class RunRoutingExplanation(
  nodeId: String,
  nodeName: String,
  intent: Option[String] = None,
  nextNodeId: Option[String] = None,
  confidence: Option[Double] = None,
  mode: String = "llm"
)

case class RunContextExplanation(
  nodeId: String,
  nodeName: String,
  selectedContexts: List[Map[String, Any]] = Nil
)

case class RunToolCallExplanation(
  nodeId: String,
  toolName: String,
  callableRef: String,
  input: Map[String, Any] = Map.empty
)

case class RunPauseExplanation(
  nodeId: String,
  nodeName: String,
  waitingFor: Any = null,
  reason: String = "",
  metadata: Map[String, Any] = Map.empty
)

case class RunSummary(
  routingDecisionCount: Int = 0,
  toolCallCount: Int = 0,
  contextInjectionCount: Int = 0,
  pauseEventCount: Int = 0
)

case class RunExplanation(
  sessionId: String,
  query: Any,
  hopCount: Int,
  paused: Boolean,
  hops: List[RunHopExplanation] = Nil,
  routing: List[RunRoutingExplanation] = Nil,
  context: List[RunContextExplanation] = Nil,
  toolCalls: List[RunToolCallExplanation] = Nil,
  pauses: List[RunPauseExplanation] = Nil,
  summary: RunSummary = RunSummary()
)

case class TraceView(
  hops: List[TraceEvent] = Nil,
  agentStarts: List[TraceEvent] = Nil,
  agentEnds: List[TraceEvent] = Nil,
  toolCalls: List[TraceEvent] = Nil,
  toolResults: List[TraceEvent] = Nil,
  contextInject: List[TraceEvent] = Nil,
  routing: List[TraceEvent] = Nil,
  pauses: List[TraceEvent] = Nil
)

def extractTraceView(events: List[TraceEvent]): TraceView = {
  val byType = events.groupBy(_.eventType)
  def pick(t: EventType): List[TraceEvent] = byType.getOrElse(t, Nil)
  TraceView(
    hops = pick(EventType.Hop),
    agentStarts = pick(EventType.AgentStart),
    agentEnds = pick(EventType.AgentEnd),
    toolCalls = pick(EventType.ToolCall),
    toolResults = pick(EventType.ToolResult),
    contextInject = pick(EventType.ContextInject),
    routing = pick(EventType.Routing),
    pauses = pick(EventType.Pause)
  )
}

def explainRun(ctx: ExecutionContext): RunExplanation = {
  val view = extractTraceView(ctx.trace)
  val paused = ctx.isPaused || view.pauses.nonEmpty
  
  RunExplanation(
    sessionId = ctx.sessionId,
    query = ctx.query,
    hopCount = ctx.hopCount,
    paused = paused,
    hops = view.hops.map { e =>
      RunHopExplanation(
        nodeId = e.nodeId,
        nodeName = e.nodeName,
        hop = e.payload.get("hop").map(_.toString.toInt),
        summary = e.payload.get("summary").map(_.toString).getOrElse("")
      )
    },
    routing = view.routing.map { e =>
      RunRoutingExplanation(
        nodeId = e.nodeId,
        nodeName = e.nodeName,
        intent = e.payload.get("intent").map(_.toString),
        nextNodeId = e.payload.get("next_node_id").map(_.toString),
        confidence = e.payload.get("confidence").collect { case n: Number => n.doubleValue },
        mode = e.payload.get("mode").map(_.toString).getOrElse("llm")
      )
    },
    context = view.contextInject.map { e =>
      RunContextExplanation(
        nodeId = e.nodeId,
        nodeName = e.nodeName,
        selectedContexts = e.payload.get("selected_contexts").map(_.asInstanceOf[List[Map[String, Any]]]).getOrElse(Nil)
      )
    },
    toolCalls = view.toolCalls.map { e =>
      RunToolCallExplanation(
        nodeId = e.nodeId,
        toolName = e.payload.get("tool_name").map(_.toString).getOrElse(e.nodeName),
        callableRef = e.payload.get("callable_ref").map(_.toString).getOrElse(""),
        input = e.payload.get("input").map(_.asInstanceOf[Map[String, Any]]).getOrElse(Map.empty)
      )
    },
    pauses = view.pauses.map { e =>
      RunPauseExplanation(
        nodeId = e.nodeId,
        nodeName = e.nodeName,
        waitingFor = e.payload.get("waiting_for").orNull,
        reason = e.payload.get("reason").map(_.toString).getOrElse(""),
        metadata = e.payload.get("metadata").map(_.asInstanceOf[Map[String, Any]]).getOrElse(Map.empty)
      )
    },
    summary = RunSummary(
      routingDecisionCount = view.routing.length,
      toolCallCount = view.toolCalls.length,
      contextInjectionCount = view.contextInject.length,
      pauseEventCount = view.pauses.length
    )
  )
}
