package loki.engine.core.nodes

import zio.schema.*
import zio.*
import java.time.Instant
import java.util.UUID

enum NodeType(val value: String) {
  case Agent     extends NodeType("agent")
  case Tool      extends NodeType("tool")
  case Context   extends NodeType("context")
  case Prompt    extends NodeType("prompt")
  case Schema    extends NodeType("schema")
  case Graph     extends NodeType("graph")
  case Approval  extends NodeType("approval")
  case Transform extends NodeType("transform")
  case Reasoner  extends NodeType("reasoner")
}

object NodeType {
  given schema: zio.schema.Schema[NodeType] = zio.schema.Schema[String].transform(v => NodeType.valueOf(v), _.value)
}

enum EdgeType(val value: String) {
  case HAS_TOOL    extends EdgeType("HAS_TOOL")
  case HAS_CONTEXT extends EdgeType("HAS_CONTEXT")
  case HAS_PROMPT  extends EdgeType("HAS_PROMPT")
  case DELEGATES_TO extends EdgeType("DELEGATES_TO")
  case PRODUCES    extends EdgeType("PRODUCES")
  case MENTIONS    extends EdgeType("MENTIONS")
  case NEXT        extends EdgeType("NEXT")
  case COVERS      extends EdgeType("COVERS")
  case SIMILAR_TO  extends EdgeType("SIMILAR_TO")
  case VALIDATES   extends EdgeType("VALIDATES")
  case CONTAINS    extends EdgeType("CONTAINS")
  // Domain edge — consumer-defined knowledge-graph relationship. Domain-agnostic:
  // the specific relation name lives in Edge.relation; loki never interprets it.
  case RELATES     extends EdgeType("RELATES")
}

object EdgeType {
  given schema: zio.schema.Schema[EdgeType] = zio.schema.Schema[String].transform(v => EdgeType.valueOf(v), _.value)
}

enum EventType(val value: String) {
  case AgentStart      extends EventType("agent_start")
  case AgentEnd        extends EventType("agent_end")
  // An agent that was not run at all because every completion path it declared
  // already exists — see GraphExecutor.missingCompletionPaths.
  case AgentSkip       extends EventType("agent_skip")
  case ToolCall        extends EventType("tool_call")
  case ToolResult      extends EventType("tool_result")
  case Routing         extends EventType("routing")
  case ContextInject   extends EventType("context_inject")
  case Hop             extends EventType("hop")
  case SubgraphEnter   extends EventType("subgraph_enter")
  case SubgraphExit    extends EventType("subgraph_exit")
  case Pause           extends EventType("pause")
  case Resume          extends EventType("resume")
  case Retry           extends EventType("retry")
  case Validation      extends EventType("validation")
  case PermissionDenied extends EventType("permission_denied")
  case Checkpoint      extends EventType("checkpoint")
  case Transaction     extends EventType("transaction")
  case ApprovalTask   extends EventType("approval_task")
  case Lease           extends EventType("lease")
  case Schedule        extends EventType("schedule")
  case Migration       extends EventType("migration")
  case Error           extends EventType("error")
}

object EventType {
  given schema: zio.schema.Schema[EventType] = zio.schema.Schema[String].transform(v => EventType.valueOf(v), _.value)
}

enum BatchStatus(val value: String) {
  case Pending   extends BatchStatus("pending")
  case Running   extends BatchStatus("running")
  case Completed extends BatchStatus("completed")
  case Failed    extends BatchStatus("failed")
  case Partial   extends BatchStatus("partial")
}

object BatchStatus {
  given schema: zio.schema.Schema[BatchStatus] = zio.schema.Schema[String].transform(v => BatchStatus.valueOf(v), _.value)
}
