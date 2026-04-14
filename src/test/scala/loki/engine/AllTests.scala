package loki.engine

import zio.test.{test, assert, Assertion, Spec, suite, ZIOSpecDefault}

object AllTests extends ZIOSpecDefault {
  def spec = suite("LokiEngineTests")(
    NodeTests.spec,
    EdgeTests.spec,
    GraphStoreTests.spec,
    ExecutorTests.spec,
    HistoryCompactionTests.spec,
    GraphAppTests.spec,
    ObservabilityTests.spec,
    BatchExecutorTests.spec,
    ToolAndBackendTests.spec,
    OllamaIntegrationTests.spec,
    MultiAgentContextTests.spec,
    SymbolicTests.spec,
    ReasonerNodeTests.spec,
    LocalModelToleranceTests.spec,
    CodeQualityTests.spec
  )
}
