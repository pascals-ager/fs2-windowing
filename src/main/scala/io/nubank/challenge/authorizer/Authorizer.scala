package io.nubank.challenge.authorizer

import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp}
import cats.effect.implicits._
import fs2.Stream

object Authorizer extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    Stream
      .resource(Blocker[IO])
      .flatMap { blocker => fs2.io.stdinUtf8[IO](4096, blocker).evalTap(x => IO.delay(println(x))) }
      .compile
      .drain
      .as(ExitCode.Success)
}
