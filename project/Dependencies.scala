import sbt._

object Dependencies {

  object Versions {
    val fs2 = "2.5.0"
    val cache = "0.28.0"
    val circeVersion = "0.13.0"
    val scalatest = "3.1.1"
  }

  lazy val fs2 = Seq(
    "co.fs2" %% "fs2-core" % Versions.fs2,
    "co.fs2" %% "fs2-io" % Versions.fs2
  )

  lazy val cache = Seq(
    "com.github.cb372" %% "scalacache-guava" % Versions.cache,
    "com.github.cb372" %% "scalacache-cats-effect" % Versions.cache
  )

  lazy val circe = Seq(
    "io.circe" %% "circe-parser" % Versions.circeVersion
  )

  lazy val scalatest = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalatest % Test
  )

}