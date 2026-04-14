ThisBuild / scalaVersion := "3.5.2"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "loki"

val zioVersion = "2.1.9"
val zioSchemaVersion = "1.0.0"

lazy val lokiEngine = project
  .in(file("."))
  .settings(
    name := "loki-engine",
    scalaVersion := "3.5.2",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-schema" % zioSchemaVersion,
      "dev.zio" %% "zio-schema-json" % zioSchemaVersion,
      "dev.zio" %% "zio-http" % "3.0.1",
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
      "dev.zio" %% "zio-test-junit" % zioVersion % Test,
      "io.netty" % "netty-handler" % "4.1.115.Final",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0",
    ),
    dependencyOverrides += "dev.zio" %% "zio" % zioVersion,
    dependencyOverrides += "dev.zio" %% "zio-streams" % zioVersion,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
