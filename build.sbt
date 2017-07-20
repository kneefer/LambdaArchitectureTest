import Dependencies._

name := "LambdaArchitectureTest"

spName := "sbartnik/LambdaArchitectureTest"

scalacOptions += "-feature"

sparkVersion := "2.0.0"

retrieveManaged := true

sparkComponents ++= Seq("core","streaming", "sql")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

spIncludeMaven := true

lazy val commonSettings = Seq(
  organization := "com.sbartnik",
  scalaVersion := "2.11.8",
  version := "0.1.0"
)

lazy val deps = Seq(
  kafka,
  akkaHttp,
  lift,
  sparkHive,
  sparkStreamingKafka,
  sparkCassandraConnect,
  cassandraDriver,
  logback,
  akkaHttpJson,
  jansi,
  json4s,
  tranquilityCore,
  tranquilitySpark
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "LambdaArchitectureTest",
    libraryDependencies ++= deps
  )