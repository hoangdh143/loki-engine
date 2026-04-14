package loki.engine.core.nodes

import zio.schema.*
import zio.*
import java.time.Instant
import java.util.UUID
import scala.collection.mutable.Map as MMap

case class ConstraintRule(
  name: String = "",
  source: String = "state",
  path: String = "",
  operator: String = "equals",
  value: Any = null.asInstanceOf[Any],
  compareToSource: Option[String] = None,
  compareToPath: String = "",
  message: String = "",
  severity: String = "error"
)

case class DecisionRule(
  name: String = "",
  conditions: List[ConstraintRule] = Nil,
  targetNodeId: String = "__END__",
  priority: Int = 0,
  metadata: Map[String, Any] = Map.empty
)

case class DecisionTable(
  name: String = "",
  rules: List[DecisionRule] = Nil,
  defaultTargetId: String = "__END__",
  defaultIntent: String = "decision_table_default",
  strict: Boolean = false
)

case class RetryPolicy(
  maxAttempts: Int = 1,
  backoffSeconds: Double = 0.0,
  backoffMultiplier: Double = 2.0
)

case class ExecutionPolicy(
  timeoutSeconds: Option[Double] = None,
  retryPolicy: RetryPolicy = RetryPolicy(),
  idempotencyKey: String = "",
  transactionBoundary: String = ""
)

case class RouteRule(
  name: String = "",
  source: String = "result",
  path: String = "",
  operator: String = "equals",
  value: Any = null.asInstanceOf[Any],
  targetNodeId: String = "__END__",
  priority: Int = 0,
  pauseOnMatch: Boolean = false,
  metadata: Map[String, Any] = Map.empty
)

sealed trait Node {
  val nodeId: String
  val nodeType: NodeType
  val name: String
  val description: String
  val embedding: Option[List[Double]]
  val validAt: Instant
  val invalidAt: Option[Instant]
  val attributes: Map[String, Any]
  val groupId: String
  val version: Int
  val graphVersion: String
  // Optional consumer-defined domain subtype for knowledge-graph nodes
  // (e.g. "customer", "order", "product"). Domain-agnostic: loki never
  // interprets this value — it lets consumers layer a typed domain graph
  // (typically on Context nodes) without widening the NodeType enum.
  val domainType: Option[String]

  def isValid: Boolean = {
    val now = Instant.now()
    validAt.isBefore(now) && (invalidAt.isEmpty || invalidAt.get.isAfter(now))
  }

  def expire(): Node = this match {
    case n: AgentNode     => n.copy(invalidAt = Some(Instant.now()))
    case n: ToolNode      => n.copy(invalidAt = Some(Instant.now()))
    case n: ContextNode   => n.copy(invalidAt = Some(Instant.now()))
    case n: PromptNode    => n.copy(invalidAt = Some(Instant.now()))
    case n: SchemaNode    => n.copy(invalidAt = Some(Instant.now()))
    case n: GraphNode     => n.copy(invalidAt = Some(Instant.now()))
    case n: ApprovalNode  => n.copy(invalidAt = Some(Instant.now()))
    case n: TransformNode => n.copy(invalidAt = Some(Instant.now()))
    case n: ReasonerNode  => n.copy(invalidAt = Some(Instant.now()))
    case n: BaseNode      => n.copy(invalidAt = Some(Instant.now()))
  }
}

case class BaseNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Context,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  domainType: Option[String] = None
) extends Node

case class AgentNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Agent,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  domainType: Option[String] = None,
  model: String = "claude-sonnet-4-6",
  systemPrompt: String = "",
  maxIterations: Int = 10,
  maxTokens: Int = 8096,
  routingTable: Map[String, String] = Map("default" -> "__END__"),
  routeRules: List[RouteRule] = Nil,
  decisionTable: Option[DecisionTable] = None,
  executionPolicy: ExecutionPolicy = ExecutionPolicy(),
  stateSchema: Map[String, Any] = Map.empty,
  constraintRules: List[ConstraintRule] = Nil,
  pauseBefore: Boolean = false,
  pauseAfter: Boolean = false,
  waitForInput: Option[String] = None
) extends Node

case class ToolNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Tool,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  domainType: Option[String] = None,
  inputSchema: Map[String, Any] = Map.empty,
  outputSchema: Map[String, Any] = Map.empty,
  callableRef: String = "",
  isAsync: Boolean = true,
  tags: List[String] = Nil,
  executionPolicy: ExecutionPolicy = ExecutionPolicy()
) extends Node {
  def toToolSchema: Map[String, Any] = Map(
    "name" -> name,
    "description" -> description,
    "input_schema" -> (if (inputSchema.isEmpty) Map("type" -> "object", "properties" -> Map.empty) else inputSchema)
  )
}

case class ContextNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Context,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  domainType: Option[String] = None,
  content: String = "",
  contentType: String = "text",
  source: String = "",
  tokenCount: Int = 0,
  tags: List[String] = Nil,
  priority: Int = 0,
  factValidAt: Option[Instant] = None,
  factInvalidAt: Option[Instant] = None
) extends Node {
  def isFactValid: Boolean = {
    val now = Instant.now()
    (factValidAt.isEmpty || factValidAt.get.isBefore(now)) &&
    (factInvalidAt.isEmpty || factInvalidAt.get.isAfter(now))
  }
}

case class PromptNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Prompt,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  domainType: Option[String] = None,
  template: String = "",
  variables: List[String] = Nil
) extends Node {
  def render(vars: Map[String, Any]): String =
    vars.foldLeft(template)((acc, kv) => acc.replace(s"{{${kv._1}}}", kv._2.toString))
}

case class SchemaNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Schema,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  domainType: Option[String] = None,
  jsonSchema: Map[String, Any] = Map.empty
) extends Node

case class GraphNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Graph,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  domainType: Option[String] = None,
  entryNodeId: String = "",
  exitNodeId: String = "",
  strategy: String = "sequential",
  inputKeys: List[String] = Nil,
  inputMap: Map[String, String] = Map.empty,
  scopeOutputs: Boolean = true,
  executionPolicy: ExecutionPolicy = ExecutionPolicy()
) extends Node

case class ApprovalNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Approval,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  domainType: Option[String] = None,
  instructions: String = "",
  assignees: List[String] = Nil,
  assignmentMode: String = "any",
  slaSeconds: Option[Int] = None,
  escalationTarget: String = "",
  inputKey: String = "approval",
  approvedTargetId: String = "__END__",
  rejectedTargetId: String = "__END__",
  requireAssignment: Boolean = false
) extends Node

/** Pure data reshaping step with no LLM invocation.
 *
 *  Sits between nodes in a pipeline to split, join, filter, or reformat
 *  data without burning tokens.  The registered callable receives a
 *  Map[String, Any] of collected inputs and returns Any, which flows into
 *  ctx.outputs and, when output_key is set, also into ctx.state.data.
 *
 *  inputKeys — node IDs or state.data keys to collect before running.
 *    When non-empty the topological executor treats them as hard
 *    dependencies (fan-in), guaranteeing all listed branches finish before
 *    this node fires.
 *  outputKey — if set, also writes the result to ctx.state.data[outputKey]
 *    so downstream agents can read it from workflow state.
 */
case class TransformNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Transform,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  domainType: Option[String] = None,
  callableRef: String = "",
  inputKeys: List[String] = Nil,
  outputKey: String = "",
  isAsync: Boolean = true,
  executionPolicy: ExecutionPolicy = ExecutionPolicy()
) extends Node

/** Declares where a ReasonerNode pulls ground facts from. */
case class FactSource(
  stateKeys: List[String] = Nil,
  edgeTypes: List[String] = Nil,
  includeNodeFacts: Boolean = false,
  useNodeNames: Boolean = true
)

/** A symbolic inference step backed by the built-in Datalog engine.
 *
 *  Runs a deterministic, sound rule program over ground facts gathered
 *  from workflow state and/or the knowledge graph, computes the deductive
 *  closure, and writes the result back for a downstream agent to consume.
 *  No LLM is invoked — this is the verifiable, explainable half of
 *  neurosymbolic AI.
 */
case class ReasonerNode(
  nodeId: String = UUID.randomUUID().toString,
  nodeType: NodeType = NodeType.Reasoner,
  name: String = "",
  description: String = "",
  embedding: Option[List[Double]] = None,
  validAt: Instant = Instant.now(),
  invalidAt: Option[Instant] = None,
  attributes: Map[String, Any] = Map.empty,
  groupId: String = "default",
  version: Int = 1,
  graphVersion: String = "v1",
  domainType: Option[String] = None,
  program: String = "",
  rules: List[Map[String, Any]] = Nil,
  factSource: FactSource = FactSource(),
  query: List[String] = Nil,
  emitDerivedOnly: Boolean = true,
  withProof: Boolean = false,
  outputKey: String = "inferred",
  failOnEmpty: Boolean = false,
  executionPolicy: ExecutionPolicy = ExecutionPolicy()
) extends Node

type AnyNode = AgentNode | ToolNode | ContextNode | PromptNode | SchemaNode | GraphNode | ApprovalNode | TransformNode | ReasonerNode | BaseNode

object AnyNode {
  def fromMap(data: Map[String, Any]): AnyNode = {
    val nodeType = data.get("node_type").flatMap(v => NodeType.values.find(_.value == v.toString)).getOrElse(NodeType.Context)
    val dt = data.get("domain_type").map(_.toString)
    nodeType match {
      case NodeType.Agent => AgentNode(
        nodeId = data.get("node_id").map(_.toString).getOrElse(UUID.randomUUID().toString),
        name = data.get("name").map(_.toString).getOrElse(""),
        description = data.get("description").map(_.toString).getOrElse(""),
        model = data.get("model").map(_.toString).getOrElse("claude-sonnet-4-6"),
        systemPrompt = data.get("system_prompt").map(_.toString).getOrElse(""),
        maxIterations = data.get("max_iterations").flatMap(v => v.toString.toIntOption).getOrElse(10),
        maxTokens = data.get("max_tokens").flatMap(v => v.toString.toIntOption).getOrElse(8096),
        domainType = dt
      )
      case NodeType.Tool => ToolNode(
        nodeId = data.get("node_id").map(_.toString).getOrElse(UUID.randomUUID().toString),
        name = data.get("name").map(_.toString).getOrElse(""),
        description = data.get("description").map(_.toString).getOrElse(""),
        callableRef = data.get("callable_ref").map(_.toString).getOrElse(""),
        domainType = dt
      )
      case NodeType.Transform => TransformNode(
        nodeId = data.get("node_id").map(_.toString).getOrElse(UUID.randomUUID().toString),
        name = data.get("name").map(_.toString).getOrElse(""),
        description = data.get("description").map(_.toString).getOrElse(""),
        callableRef = data.get("callable_ref").map(_.toString).getOrElse(""),
        inputKeys = data.get("input_keys").map(_.asInstanceOf[List[String]]).getOrElse(Nil),
        outputKey = data.get("output_key").map(_.toString).getOrElse(""),
        isAsync = data.get("is_async").map(_.toString.toBoolean).getOrElse(true),
        domainType = dt
      )
      case NodeType.Context => ContextNode(
        nodeId = data.get("node_id").map(_.toString).getOrElse(UUID.randomUUID().toString),
        name = data.get("name").map(_.toString).getOrElse(""),
        description = data.get("description").map(_.toString).getOrElse(""),
        content = data.get("content").map(_.toString).getOrElse(""),
        domainType = dt
      )
      case NodeType.Prompt => PromptNode(
        nodeId = data.get("node_id").map(_.toString).getOrElse(UUID.randomUUID().toString),
        name = data.get("name").map(_.toString).getOrElse(""),
        template = data.get("template").map(_.toString).getOrElse(""),
        domainType = dt
      )
      case NodeType.Reasoner => ReasonerNode(
        nodeId = data.get("node_id").map(_.toString).getOrElse(UUID.randomUUID().toString),
        name = data.get("name").map(_.toString).getOrElse(""),
        description = data.get("description").map(_.toString).getOrElse(""),
        program = data.get("program").map(_.toString).getOrElse(""),
        outputKey = data.get("output_key").map(_.toString).getOrElse("inferred"),
        emitDerivedOnly = data.get("emit_derived_only").map(_.toString.toBoolean).getOrElse(true),
        withProof = data.get("with_proof").map(_.toString.toBoolean).getOrElse(false),
        failOnEmpty = data.get("fail_on_empty").map(_.toString.toBoolean).getOrElse(false),
        domainType = dt
      )
      case _ => BaseNode(nodeId = data.get("node_id").map(_.toString).getOrElse(UUID.randomUUID().toString), domainType = dt)
    }
  }
}
