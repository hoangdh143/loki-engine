package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite}
import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.edges.*
import loki.engine.core.store.*

object GraphStoreTests {

  def spec = suite("GraphStoreTests")(
    test("InMemoryGraphStore should upsert and get nodes") {
      val store = new InMemoryGraphStore()
      val agent = AgentNode(name = "TestAgent")
      
      for {
        _ <- store.upsertNode(agent)
        retrieved <- store.getNode(agent.nodeId)
      } yield {
        assert(retrieved.isDefined)(Assertion.equalTo(true)) &&
        assert(retrieved.get.name)(Assertion.equalTo("TestAgent"))
      }
    },
    
    test("InMemoryGraphStore should upsert and get edges") {
      val store = new InMemoryGraphStore()
      val edge = Edge.hasTool("a", "b")
      
      for {
        _ <- store.upsertEdge(edge)
        retrieved <- store.getEdge(edge.edgeId)
      } yield {
        assert(retrieved.isDefined)(Assertion.equalTo(true)) &&
        assert(retrieved.get.edgeType)(Assertion.equalTo(EdgeType.HAS_TOOL))
      }
    },
    
    test("InMemoryGraphStore should list nodes by type") {
      val store = new InMemoryGraphStore()
      val agent = AgentNode(name = "Agent1")
      val tool = ToolNode(name = "Tool1")
      val ctx = ContextNode(name = "Ctx1")
      
      for {
        _ <- store.upsertNode(agent)
        _ <- store.upsertNode(tool)
        _ <- store.upsertNode(ctx)
        agents <- store.listNodes(Some(NodeType.Agent))
        tools <- store.listNodes(Some(NodeType.Tool))
      } yield {
        assert(agents.length)(Assertion.equalTo(1)) &&
        assert(agents.head.name)(Assertion.equalTo("Agent1")) &&
        assert(tools.length)(Assertion.equalTo(1)) &&
        assert(tools.head.name)(Assertion.equalTo("Tool1"))
      }
    },
    
    test("InMemoryGraphStore should get edges by type") {
      val store = new InMemoryGraphStore()
      val edge1 = Edge.hasTool("a", "b")
      val edge2 = Edge.hasContext("a", "c")
      
      for {
        _ <- store.upsertEdge(edge1)
        _ <- store.upsertEdge(edge2)
        toolEdges <- store.getEdges("a", Some(EdgeType.HAS_TOOL))
        contextEdges <- store.getEdges("a", Some(EdgeType.HAS_CONTEXT))
      } yield {
        assert(toolEdges.length)(Assertion.equalTo(1)) &&
        assert(contextEdges.length)(Assertion.equalTo(1))
      }
    },
    
    test("InMemoryGraphStore should delete nodes and edges") {
      val store = new InMemoryGraphStore()
      val agent = AgentNode(name = "ToDelete")
      val edge = Edge.hasTool("agent", "tool")
      
      for {
        _ <- store.upsertNode(agent)
        _ <- store.upsertEdge(edge)
        _ <- store.deleteNode(agent.nodeId)
        _ <- store.deleteEdge(edge.edgeId)
        retrievedNode <- store.getNode(agent.nodeId)
        retrievedEdge <- store.getEdge(edge.edgeId)
      } yield {
        assert(retrievedNode)(Assertion.equalTo(None)) &&
        assert(retrievedEdge)(Assertion.equalTo(None))
      }
    },
    
    test("InMemoryGraphStore should expire nodes") {
      val store = new InMemoryGraphStore()
      val agent = AgentNode(name = "ToExpire")
      
      for {
        _ <- store.upsertNode(agent)
        _ <- store.expireNode(agent.nodeId)
        validNodes <- store.listNodes(onlyValid = true)
        allNodes <- store.listNodes(onlyValid = false)
      } yield {
        assert(validNodes.length)(Assertion.equalTo(0)) &&
        assert(allNodes.length)(Assertion.equalTo(1))
      }
    },
    
    test("InMemoryGraphStore should attach context with auto-weight") {
      val store = new InMemoryGraphStore()
      val agent = AgentNode(name = "Agent", embedding = Some(List(0.1, 0.2, 0.3)))
      val ctx = ContextNode(name = "Context", embedding = Some(List(0.1, 0.2, 0.3)))
      
      for {
        _ <- store.upsertNode(agent)
        _ <- store.upsertNode(ctx)
        edge <- store.attachContext(agent.nodeId, ctx.nodeId)
      } yield {
        assert(edge.edgeType)(Assertion.equalTo(EdgeType.HAS_CONTEXT)) &&
        assert(math.abs(edge.weight - 1.0) < 1e-9)(Assertion.equalTo(true))
      }
    },
    
    test("InMemoryGraphStore should vector search") {
      val store = new InMemoryGraphStore()
      val ctx1 = ContextNode(name = "Cat", embedding = Some(List(1.0, 0.0, 0.0)))
      val ctx2 = ContextNode(name = "Dog", embedding = Some(List(0.0, 1.0, 0.0)))
      val ctx3 = ContextNode(name = "Car", embedding = Some(List(0.0, 0.0, 1.0)))
      
      for {
        _ <- store.upsertNode(ctx1)
        _ <- store.upsertNode(ctx2)
        _ <- store.upsertNode(ctx3)
        results <- store.vectorSearch(List(1.0, 0.0, 0.0), topK = 2)
      } yield {
        assert(results.length)(Assertion.equalTo(2)) &&
        assert(results.head._1.asInstanceOf[ContextNode].name)(Assertion.equalTo("Cat"))
      }
    },
    
    test("InMemoryGraphStore should get neighbors") {
      val store = new InMemoryGraphStore()
      val agent = AgentNode(name = "Agent")
      val tool1 = ToolNode(name = "Tool1")
      val tool2 = ToolNode(name = "Tool2")
      
      for {
        _ <- store.upsertNode(agent)
        _ <- store.upsertNode(tool1)
        _ <- store.upsertNode(tool2)
        _ <- store.attachTool(agent.nodeId, tool1.nodeId)
        _ <- store.attachTool(agent.nodeId, tool2.nodeId)
        neighbors <- store.neighbors(agent.nodeId)
      } yield {
        assert(neighbors.length)(Assertion.equalTo(2))
      }
    },
    
    test("GraphStore.make should create new store") {
      for {
        store <- GraphStore.make
        agent = AgentNode(name = "MakeTest")
        _ <- store.upsertNode(agent)
        retrieved <- store.getNode(agent.nodeId)
      } yield {
        assert(retrieved.isDefined)(Assertion.equalTo(true))
      }
    },

    // Domain knowledge-graph layer: a typed
    // domain graph coexisting with the orchestration graph in the same store,
    // filtered by consumer-defined domain_type / relation labels.
    test("InMemoryGraphStore should list nodes by domainType") {
      val store = new InMemoryGraphStore()
      val customer = ContextNode(name = "Alice", domainType = Some("customer"))
      val order    = ContextNode(name = "Order#1", domainType = Some("order"))
      val product  = ContextNode(name = "Widget", domainType = Some("product"))

      for {
        _ <- store.upsertNode(customer)
        _ <- store.upsertNode(order)
        _ <- store.upsertNode(product)
        customers <- store.listNodes(domainType = Some("customer"))
        orders    <- store.listNodes(domainType = Some("order"))
        all       <- store.listNodes()
      } yield {
        assert(customers.map(_.name))(Assertion.equalTo(List("Alice"))) &&
        assert(orders.map(_.name))(Assertion.equalTo(List("Order#1"))) &&
        assert(all.length)(Assertion.equalTo(3))
      }
    },

    test("InMemoryGraphStore should filter RELATES edges by relation") {
      val store = new InMemoryGraphStore()
      val placed = Edge.relates("customer", "order", "placed")
      val buys   = Edge.relates("order", "product", "contains")

      for {
        _ <- store.upsertEdge(placed)
        _ <- store.upsertEdge(buys)
        placedEdges  <- store.getEdges("customer", Some(EdgeType.RELATES), relation = Some("placed"))
        listContains <- store.listEdges(Some(EdgeType.RELATES), relation = Some("contains"))
        allRelates   <- store.listEdges(Some(EdgeType.RELATES))
      } yield {
        assert(placedEdges.length)(Assertion.equalTo(1)) &&
        assert(placedEdges.head.relation)(Assertion.equalTo(Some("placed"))) &&
        assert(listContains.length)(Assertion.equalTo(1)) &&
        assert(listContains.head.dstId)(Assertion.equalTo("product")) &&
        assert(allRelates.length)(Assertion.equalTo(2))
      }
    }
  )
}