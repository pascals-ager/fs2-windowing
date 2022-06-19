import Dependencies._

lazy val commonSettings = Seq(
  name := "authorizerfs2",
  scalaVersion := "2.13.4",
  version := "0.1",
  organization := "io.win.stream",
  scalacOptions ++= Seq(
    // warnings
    "-unchecked", // able additional warnings where generated code depends on assumptions
    "-deprecation", // emit warning for usages of deprecated APIs
    "-feature", // emit warning usages of features that should be imported explicitly
    // Features enabled by default
    "-language:higherKinds",
    "-language:implicitConversions",
    "-language:experimental.macros",
    // possibly deprecated optionsyes
    "-Ywarn-dead-code",
    "-language:higherKinds",
    "-language:existentials",
    "-Ywarn-extra-implicit"
  )
)

resolvers ++= Seq(
  "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases",
  Resolver.sonatypeRepo("releases")
)


lazy val authorizerfs2 = (project in file(".")).
  enablePlugins(JavaServerAppPackaging,
    AshScriptPlugin,
    DockerPlugin).
  settings(moduleName := "authorizerfs2").
  settings(mainClass in Compile := Some("io.win.stream.authorizer.Authorizer")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= fs2 ++ circe ++ scalatest ++ log4cats ++ monix ++ typedconfig
  )

scalafmtOnCompile := true

/* Default the image is built on openjdk11 */
dockerBaseImage := "adoptopenjdk/openjdk11"
daemonUser in Docker    := "authorizerfs2"
/*
* Customize this for default window size
* */
dockerEnvVars := Map(
  "TIME_WINDOW_SIZE_SECONDS" -> "120",
  "TOPIC_QUEUE_SIZE" -> "10",
  "TRANSACTION_FREQUENCY_TOLERANCE" -> "3",
  "TRANSACTION_DOUBLED_TOLERANCE" -> "1"
)