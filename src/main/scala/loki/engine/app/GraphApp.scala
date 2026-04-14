package loki.engine.app

import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.core.executor.*
import loki.engine.backend.*
import scala.collection.mutable

type NodeRef = String | AgentNode | ToolNode | ContextNode | PromptNode

val END_NODE = "__END__"

def createAgent(
  name: String,
  description: String = "",
  model: String = "claude-sonnet-4-6",
  systemPrompt: String = "",
  routingTable: Option[Map[String, String]] = None,
  maxIterations: Int = 10,
  maxTokens: Int = 8096
): AgentNode = AgentNode(
  name = name,
  description = description,
  model = model,
  systemPrompt = systemPrompt,
  routingTable = routingTable.getOrElse(Map("default" -> END_NODE)),
  maxIterations = maxIterations,
  maxTokens = maxTokens
)

def createTool(
  name: String,
  callableRef: String,
  description: String = "",
  inputSchema: Map[String, Any] = Map.empty,
  outputSchema: Map[String, Any] = Map.empty,
  isAsync: Boolean = true
): ToolNode = ToolNode(
  name = name,
  description = description,
  callableRef = callableRef,
  inputSchema = inputSchema,
  outputSchema = outputSchema,
  isAsync = isAsync
)

def createContext(
  name: String,
  content: String,
  description: String = ""
): ContextNode = ContextNode(
  name = name,
  content = content,
  description = description
)

def createPrompt(
  name: String,
  template: String,
  description: String = ""
): PromptNode = PromptNode(
  name = name,
  template = template,
  description = description
)

def createExecutor(
  store: GraphStore = InMemoryGraphStore(),
  backend: LLMBackend = new DefaultLLMBackend,
  routerModel: String = "claude-haiku-4-5-20251001"
): GraphExecutor = new GraphExecutor(
  store = store,
  backend = backend,
  composer = new AgentComposer(store),
  routerModel = routerModel
)

class GraphApp(
  store: GraphStore = InMemoryGraphStore(),
  executor: Option[GraphExecutor] = None,
  backend: Option[LLMBackend] = None
) {
  private val toolFns = mutable.Map[String, Any => Any]()

  /** Created on first use so tools registered before then are included;
    * matching the original lazy-init semantics, tools registered after the
    * executor exists are not replayed onto it. */
  private lazy val resolvedExecutor: GraphExecutor = {
    val exec = executor.getOrElse {
      new GraphExecutor(
        store = store,
        backend = backend.getOrElse(new DefaultLLMBackend),
        composer = new AgentComposer(store)
      )
    }
    for ((ref, fn) <- toolFns) exec.registerTool(ref, fn)
    exec
  }

  def getExecutor: GraphExecutor = resolvedExecutor

  def addAgent(name: String, agent: AgentNode): UIO[AgentNode] = 
    store.upsertNode(agent).as(agent)

  def addTool(name: String, tool: ToolNode, fn: Option[Any => Any] = None): UIO[ToolNode] = {
    fn.foreach(toolFns.put(tool.callableRef, _))
    store.upsertNode(tool).as(tool)
  }

  def addContext(name: String, ctx: ContextNode): UIO[ContextNode] =
    store.upsertNode(ctx).as(ctx)

  def addPrompt(name: String, prompt: PromptNode): UIO[PromptNode] =
    store.upsertNode(prompt).as(prompt)

  def registerTool(callableRef: String, fn: Any => Any): Unit =
    toolFns.put(callableRef, fn)

  def useDefaultTools(): Unit = {
    import loki.engine.tools.DefaultTools
    DefaultTools.registry.items.foreach { case (ref, fn) => toolFns.put(ref, fn) }
  }

  def connectTool(agent: NodeRef, tool: NodeRef, weight: Option[Double] = None): UIO[Edge] =
    store.attachTool(nodeId(agent), nodeId(tool), weight)

  def connectContext(agent: NodeRef, ctx: NodeRef, weight: Option[Double] = None): UIO[Edge] =
    store.attachContext(nodeId(agent), nodeId(ctx), weight)

  def connectPrompt(agent: NodeRef, prompt: NodeRef): UIO[Edge] =
    store.upsertEdge(Edge.hasPrompt(nodeId(agent), nodeId(prompt))).as(Edge.hasPrompt(nodeId(agent), nodeId(prompt)))

  def delegate(srcAgent: NodeRef, dstAgent: NodeRef): UIO[Edge] =
    store.upsertEdge(Edge.delegatesTo(nodeId(srcAgent), nodeId(dstAgent))).as(Edge.delegatesTo(nodeId(srcAgent), nodeId(dstAgent)))

  def run(entryNode: NodeRef, query: Any, strategy: String = "sequential"): ZIO[Any, Throwable, ExecutionContext] =
    getExecutor.run(nodeId(entryNode), query, strategy)

  private def nodeId(ref: NodeRef): String = ref match {
    case s: String => s
    case n: AgentNode => n.nodeId
    case n: ToolNode => n.nodeId
    case n: ContextNode => n.nodeId
    case n: PromptNode => n.nodeId
  }
}

class DefaultLLMBackend extends LLMBackend {
  def chat(
    model: String,
    system: String,
    messages: List[Map[String, Any]],
    tools: List[Map[String, Any]],
    maxTokens: Int = 8096
  ): Task[LLMResponse] = ZIO.succeed {
    LLMResponse(
      text = "Default backend response - no LLM configured",
      toolCalls = Nil,
      stopReason = "end_turn"
    )
  }

  def extendMessages(
    messages: List[Map[String, Any]],
    response: LLMResponse,
    toolResults: List[ToolResult]
  ): List[Map[String, Any]] = messages
}
