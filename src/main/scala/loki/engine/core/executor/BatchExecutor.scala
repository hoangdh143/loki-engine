package loki.engine.core.executor

import zio.*
import loki.engine.core.nodes.*
import loki.engine.core.store.*
import java.time.Instant
import java.util.UUID
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

case class BatchItemResult(
  itemId: String,
  status: BatchStatus = BatchStatus.Pending,
  output: Any = null,
  error: Option[String] = None,
  startedAt: Option[Instant] = None,
  endedAt: Option[Instant] = None
) {
  def durationSeconds: Option[Double] = for {
    start <- startedAt
    end <- endedAt
  } yield (end.getEpochSecond - start.getEpochSecond).toDouble
}

case class BatchRun(
  runId: String,
  agentId: String,
  total: Int,
  results: Map[String, BatchItemResult] = Map.empty,
  reducedOutput: Any = null,
  startedAt: Instant = Instant.now(),
  endedAt: Option[Instant] = None
) {
  def completed: Int = results.values.count(_.status == BatchStatus.Completed)
  def failed: Int = results.values.count(_.status == BatchStatus.Failed)
  def pending: Int = results.values.count(_.status == BatchStatus.Pending)
  def running: Int = results.values.count(_.status == BatchStatus.Running)
  
  def status: BatchStatus = {
    if (endedAt.isEmpty) BatchStatus.Running
    else if (failed == 0) BatchStatus.Completed
    else if (completed == 0) BatchStatus.Failed
    else BatchStatus.Partial
  }
  
  def progress: Double = {
    if (total == 0) 1.0
    else (completed + failed).toDouble / total
  }
  
  def successfulOutputs: List[Any] = {
    results.values.filter(_.status == BatchStatus.Completed).toList.sortBy(_.itemId).map(_.output)
  }
}

class BatchExecutor(
  store: GraphStore,
  executor: GraphExecutor,
  concurrency: Int = 10
) {
  private val resultsRef = new java.util.concurrent.ConcurrentHashMap[String, BatchItemResult]()

  def run(
    agentNodeId: String,
    items: List[Any],
    queryFn: Any => String,
    contextFn: Option[Any => String] = None,
    reduceFn: Option[List[Any] => Any] = None,
    onProgress: Option[BatchRun => Any] = None,
    checkpoint: Boolean = true,
    strategy: String = "sequential"
  ): ZIO[Any, Throwable, BatchRun] = {
    val runId = UUID.randomUUID().toString
    val initialResults = (0 until items.length).map(i => i.toString -> BatchItemResult(itemId = i.toString)).toMap
    initialResults.foreach { case (k, v) => resultsRef.put(k, v) }
    runBatch(runId, agentNodeId, items, queryFn, contextFn, reduceFn, onProgress, strategy, initialResults)
  }

  def resume(
    existingRun: BatchRun,
    items: List[Any],
    queryFn: Any => String,
    contextFn: Option[Any => String] = None,
    reduceFn: Option[List[Any] => Any] = None,
    onProgress: Option[BatchRun => Any] = None
  ): ZIO[Any, Throwable, BatchRun] = {
    existingRun.results.foreach { case (k, v) => resultsRef.put(k, v) }
    runBatch(existingRun.runId, existingRun.agentId, items, queryFn, contextFn, reduceFn, onProgress, "sequential", existingRun.results)
  }

  private def runBatch(
    runId: String,
    agentNodeId: String,
    items: List[Any],
    queryFn: Any => String,
    contextFn: Option[Any => String],
    reduceFn: Option[List[Any] => Any],
    onProgress: Option[BatchRun => Any],
    strategy: String,
    initialResults: Map[String, BatchItemResult]
  ): ZIO[Any, Throwable, BatchRun] = {
    val runRef = new java.util.concurrent.atomic.AtomicReference(
      BatchRun(runId = runId, agentId = agentNodeId, total = items.length, results = initialResults)
    )

    def processItem(idx: Int, item: Any): ZIO[Any, Throwable, Unit] = {
      val itemId = idx.toString
      val existing = Option(resultsRef.get(itemId)).getOrElse(BatchItemResult(itemId = itemId))

      if (existing.status == BatchStatus.Completed) ZIO.unit
      else {
        val running = existing.copy(status = BatchStatus.Running, startedAt = Some(Instant.now()))
        resultsRef.put(itemId, running)

        (for {
          queryStr <- ZIO.succeed(queryFn(item))
          extraMessages <- ZIO.succeed(contextFn.flatMap { fn =>
            val t = fn(item)
            if (t.nonEmpty) Some(List(Map[String, Any]("role" -> "user", "content" -> t))) else None
          })
          execCtx <- executor.run(agentNodeId, queryStr, strategy = strategy, extraMessages = extraMessages)
          output = execCtx.outputs.get(agentNodeId)
          done = running.copy(status = BatchStatus.Completed, output = output, endedAt = Some(Instant.now()))
        } yield done).catchSome { case e: Throwable =>
          ZIO.succeed(running.copy(
            status = BatchStatus.Failed,
            error = Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"),
            endedAt = Some(Instant.now())
          ))
        }.flatMap { finalResult =>
          ZIO.succeed {
            resultsRef.put(itemId, finalResult)
            val newResults = resultsRef.asScala.toMap
            val newRun = runRef.get.copy(results = newResults)
            runRef.set(newRun)
            onProgress.foreach(_.apply(newRun))
          }
        }
      }
    }

    for {
      _ <- ZIO.foreachPar(items.zipWithIndex) { case (item, idx) =>
        processItem(idx, item)
      }.withParallelism(concurrency)
      currentResults = resultsRef.asScala.toMap
      finalRun = runRef.get.copy(results = currentResults, endedAt = Some(Instant.now()))
      reduced = reduceFn.map(_.apply(finalRun.successfulOutputs))
    } yield finalRun.copy(reducedOutput = reduced)
  }
}
