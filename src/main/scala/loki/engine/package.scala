package loki.engine

import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*
import loki.engine.core.executor.*
import loki.engine.backend.*
import loki.engine.retrieval.*
import loki.engine.app.*
import loki.engine.observability.*
import loki.engine.tools.*
import loki.engine.exporters.*
import loki.engine.viz.*

package object engine {
  val createAgent    = loki.engine.app.createAgent
  val createTool     = loki.engine.app.createTool
  val createContext  = loki.engine.app.createContext
  val createPrompt   = loki.engine.app.createPrompt
  val createExecutor = loki.engine.app.createExecutor
  val END_NODE = "__END__"
  type NodeRef = String | AgentNode | ToolNode | ContextNode | PromptNode
}
