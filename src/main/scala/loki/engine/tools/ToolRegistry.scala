package loki.engine.tools

import zio.*
import scala.collection.mutable

trait ToolRegistry {
  def register(callableRef: String, fn: Any => Any): Unit
  def get(callableRef: String): Option[Any => Any]
  def items: Iterator[(String, Any => Any)]
  def attach(executor: Any): Unit
  def contains(callableRef: String): Boolean
  def size: Int
}

class DefaultToolRegistry extends ToolRegistry {
  private val registry = mutable.Map[String, Any => Any]()

  def register(callableRef: String, fn: Any => Any): Unit = registry.put(callableRef, fn)
  
  def get(callableRef: String): Option[Any => Any] = registry.get(callableRef)
  
  def items: Iterator[(String, Any => Any)] = registry.iterator
  
  def attach(executor: Any): Unit = {
    val registerMethod = executor.getClass.getMethod("registerTool", classOf[String], classOf[Any])
    for ((ref, fn) <- registry) {
      registerMethod.invoke(executor, ref, fn)
    }
  }
  
  def contains(callableRef: String): Boolean = registry.contains(callableRef)
  
  def size: Int = registry.size
}

class ToolBuilder(ref: String, registry: DefaultToolRegistry) {
  def apply(fn: Any => Any): Any => Any = {
    registry.register(ref, fn)
    fn
  }
}

object EchoTool {
  def apply(input: Any): String = input.toString
  def run(input: Map[String, Any]): String = input.get("text").map(_.toString).getOrElse(input.toString)
}

object WebSearchTool {
  def apply(input: Any): String = {
    val query = input match {
      case m: Map[_, _] => m.asInstanceOf[Map[String, Any]].get("query").map(_.toString).getOrElse("")
      case _ => input.toString
    }
    s"Search results for: $query"
  }
  def run(input: Map[String, Any]): String = {
    val query = input.get("query").map(_.toString).getOrElse(input.toString)
    s"Search results for: $query"
  }
}

object CodeExecTool {
  def apply(input: Any): String = s"Executed code with input: $input"
  def run(input: Map[String, Any]): String = {
    val code = input.get("code").map(_.toString).getOrElse("")
    s"Executed code: ${code.take(100)}"
  }
}

object DefaultTools {
  val registry = new DefaultToolRegistry
  
  registry.register("loki.engine.tools.web_search.search", (input: Any) => WebSearchTool.run(input.asInstanceOf[Map[String, Any]]))
  registry.register("loki.engine.tools.echo.echo", (input: Any) => EchoTool.run(input.asInstanceOf[Map[String, Any]]))
  
  def getRegistry: ToolRegistry = registry
}
