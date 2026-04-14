# Batch Execution

Process multiple items concurrently with Loki Engine's batch processing.

## Basic Batch

```scala
import zio.*
import loki.engine.core.executor.*
import loki.engine.core.store.*
import loki.engine.app.*

val runtime = Runtime.default

Unsafe.unsafe { implicit u =>
  val store = runtime.unsafe.run(GraphStore.make).getOrThrow
  val app = new GraphApp(store)

  val documents = List(
    "Document 1 content...",
    "Document 2 content...",
    "Document 3 content..."
  )

  val batchExec = new BatchExecutor(store, app.getExecutor, concurrency = 5)

  val result = runtime.unsafe.run {
    batchExec.run(
      agentNodeId = summarizer.nodeId,
      items = documents,
      queryFn = (doc: Any) => s"Summarize: $doc"
    )
  }.getOrThrow

  println(s"Status: ${result.status}")
  println(s"Completed: ${result.completed}")
  println(s"Failed: ${result.failed}")
}
```

## With Per-Item Context

Add per-item context without graph mutation:

```scala
val result = runtime.unsafe.run {
  batchExec.run(
    agentNodeId = analyzer.nodeId,
    items = documents,
    queryFn = (doc: Document) => s"Analyze: ${doc.title}",
    contextFn = Some((doc: Document) => doc.body)  // Injected as an extra message
  )
}.getOrThrow
```

## With Map-Reduce

Combine all outputs after processing:

```scala
val result = runtime.unsafe.run {
  batchExec.run(
    agentNodeId = summarizer.nodeId,
    items = documents,
    queryFn = (doc: Any) => s"Summarize: $doc",
    reduceFn = Some((outputs: List[Any]) =>
      outputs.mkString("\n\n---\n\n")
    )
  )
}.getOrThrow

println(result.reducedOutput)  // Combined summary
```

## Progress Tracking

```scala
val result = runtime.unsafe.run {
  batchExec.run(
    agentNodeId = processor.nodeId,
    items = largeDataset,
    queryFn = itemToQuery,
    onProgress = Some((run: BatchRun) => {
      println(f"Progress: ${run.progress * 100}%1.1f%% " +
              s"(${run.completed + run.failed}/${run.total})")
    })
  )
}.getOrThrow
```

## Checkpoint and Resume

`BatchExecutor` automatically skips items that already completed. Pass the
`BatchRun` from the interrupted run to `resume()`:

```scala
// First run — may be interrupted
val run = runtime.unsafe.run {
  batchExec.run(
    agentNodeId = processor.nodeId,
    items = largeDataset,
    queryFn = itemToQuery,
    checkpoint = true
  )
}.getOrThrow

// If some items failed or the process was interrupted, resume
// Only items whose status != Completed are re-processed
if (run.status != BatchStatus.Completed) {
  val resumed = runtime.unsafe.run {
    batchExec.resume(
      existingRun = run,          // Pass the BatchRun, not just the runId
      items = largeDataset,
      queryFn = itemToQuery
    )
  }.getOrThrow

  println(s"Resumed — final status: ${resumed.status}")
}
```

## Concurrency Control

Concurrency is enforced via ZIO's `.withParallelism`:

```scala
// Low concurrency for rate-limited APIs
val gentle = new BatchExecutor(store, executor, concurrency = 2)

// High concurrency for local processing
val fast = new BatchExecutor(store, executor, concurrency = 20)

// Default — respects typical API rate limits
val standard = new BatchExecutor(store, executor, concurrency = 10)
```

## Error Handling

Individual item failures are isolated and do not stop the batch:

```scala
val result = runtime.unsafe.run {
  batchExec.run(
    agentNodeId = processor.nodeId,
    items = items,
    queryFn = itemToQuery
  )
}.getOrThrow

// Inspect per-item results
for ((itemId, itemResult) <- result.results) {
  itemResult.status match {
    case BatchStatus.Completed =>
      println(s"$itemId: OK — ${itemResult.output}")
    case BatchStatus.Failed =>
      println(s"$itemId: FAIL — ${itemResult.error.getOrElse("")}")
    case _ =>
      println(s"$itemId: ${itemResult.status}")
  }
}

// Overall status reflects partial completion
println(result.status)  // BatchStatus.Partial if some succeeded, some failed
```

## Batch Result Types

```scala
case class BatchRun(
  runId: String,
  agentId: String,
  total: Int,
  results: Map[String, BatchItemResult],
  reducedOutput: Any,
  startedAt: Instant,
  endedAt: Option[Instant]
) {
  def completed: Int                // Number of successful items
  def failed: Int                   // Number of failed items
  def pending: Int                  // Items not yet started
  def running: Int                  // Items currently in flight
  def status: BatchStatus           // Completed | Failed | Partial | Running
  def progress: Double              // (completed + failed) / total
  def successfulOutputs: List[Any]  // Outputs of Completed items, sorted by itemId
}

case class BatchItemResult(
  itemId: String,
  status: BatchStatus,
  output: Any,
  error: Option[String],
  startedAt: Option[Instant],
  endedAt: Option[Instant]
) {
  def durationSeconds: Option[Double]
}
```

## Best Practices

1. **Use `resume()` for long batches** — pass the original `BatchRun`, not just the `runId`
2. **Set concurrency to match your API rate limits** — default 10 is conservative
3. **Include a `reduceFn`** for final aggregation rather than post-processing `successfulOutputs` manually
4. **Check `result.status`** — `Partial` means some items failed; inspect `results` for details
5. **Use `onProgress`** to surface live progress in long-running batches
