import sbt._

object Dependencies {

  object Versions {
    val fs2 = "2.5.0"
    val circeVersion = "0.13.0"
    val scalatest = "3.1.1"
    val log4cats = "1.2.0-RC2"
    val monix = "3.3.0"
    val typesafe = "1.4.1"
  }

  lazy val fs2 = Seq(
    "co.fs2" %% "fs2-core" % Versions.fs2,
    "co.fs2" %% "fs2-io" % Versions.fs2
  )

  lazy val circe = Seq(
    "io.circe" %% "circe-parser" % Versions.circeVersion,
    "io.circe" %% "circe-generic" % Versions.circeVersion,
    "io.circe" %% "circe-generic-extras" % Versions.circeVersion
  )

  lazy val scalatest = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalatest % Test
  )

  lazy val log4cats = Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "net.logstash.logback" % "logstash-logback-encoder" % "6.3",
    "org.typelevel" %% "log4cats-slf4j" % Versions.log4cats
  )

  lazy val monix = Seq (
    "io.monix" %% "monix-execution" % Versions.monix
  )

  lazy val typedconfig = Seq(
    "com.typesafe" % "config" % Versions.typesafe
  )

}