# Why Behavior Changed

Common issues when migrating from other frameworks to Loki Engine.

## Synchronous → Asynchronous

**Other frameworks (Python)**: `result = await executor.run(...)`
**Loki Engine**: `result = runtime.unsafe.run(app.run(...))`

The ZIO effect system requires explicit execution.

```scala
// Correct
val result = Unsafe.unsafe { implicit u =>
  runtime.unsafe.run(app.run(agent.nodeId, query)).getOrThrow
}

// Common mistake: forgetting Unsafe
val result = runtime.unsafe.run(app.run(agent.nodeId, query)) 
// Returns Either, not the result directly
```

## Node IDs vs Names

**Other frameworks (Python)**: `await executor.run("MyAgent", query)` accepts name
**Loki Engine**: Must use `nodeId`

```scala
// Correct - use nodeId
app.run(agent.nodeId, query)

// Common mistake - using name won't work
app.run("MyAgent", query)  // Wrong!
```

## Optional Returns

ZIO uses `UIO`, `Task`, `URI` for effects that may fail or be optional.

```scala
// store.getNode returns UIO[Option[AnyNode]]
val nodeOpt = runtime.unsafe.run(store.getNode(nodeId)).getOrThrow
nodeOpt match {
  case Some(node) => // use node
  case None => // handle not found
}

// Common mistake: assuming it returns Option directly
val node = store.getNode(nodeId)  // This is UIO[Option[...]], not Option
```

## Mutable State in Context

The `ExecutionContext` is immutable - updates create new copies.

```scala
// Correct - create new context with updated state
val newCtx = ctx.copy(
  state = ctx.state.copy(
    data = ctx.state.data + ("key" -> value)
  )
)

// Common mistake - trying to mutate
ctx.state.data("key") = value  // Won't compile!
```

## Routing Table Keys

Routing tables use `__END__` for termination, not empty string or null.

```scala
// Correct
val agent = createAgent(
  routingTable = Map(
    "search" -> "searchNodeId",
    "default" -> "__END__"
  )
)

// Common mistake
Map("default" -> "")  // This means route to node with id ""
Map("default" -> null)  // Won't compile
```

## Edge Construction

Edges use factory methods, not constructor calls.

```scala
// Correct
val edge = Edge.hasTool(srcId = agent.nodeId, dstId = tool.nodeId)

// Common mistake
val edge = Edge(edgeType = EdgeType.HAS_TOOL, srcId = ..., dstId = ...)  
// This works but factory methods are preferred
```

## Tool Registration

Tools must be registered before execution.

```scala
// Correct
executor.registerTool("mymodule.search", searchFunction)
runtime.unsafe.run(app.run(agent.nodeId, query))

// Common mistake - forgetting registration
runtime.unsafe.run(app.run(agent.nodeId, query))  
// RuntimeException: Tool callable not registered
```

## Option Handling

Scala's `Option` requires explicit pattern matching.

```scala
// Correct
ctx.pendingPause match {
  case Some(pause) => println(s"Paused: ${pause.reason}")
  case None => println("Not paused")
}

// Common mistake
if (ctx.pendingPause)  // Won't compile - Option is not Boolean
```

## BatchExecutor Concurrency

Concurrency limit is for parallel tasks, not total tasks.

```scala
// Processes 100 items with max 10 running at once
val batch = new BatchExecutor(store, executor, concurrency = 10)
runtime.unsafe.run {
  batch.run(agent.nodeId, items.take(100), queryFn, checkpoint = true)
}
```

## Exception Handling

ZIO uses `ZIO.fail` for expected failures, `ZIO.die` for unexpected.

```scala
// Correct - expected failure
ZIO.fail(new IllegalArgumentException("Invalid input"))

// Unexpected - programming error
ZIO.die(new NoSuchElementException("Missing key"))
```
