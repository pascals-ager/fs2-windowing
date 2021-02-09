package io.nubank.challenge.authorizer

import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp}
import cats.effect.implicits._
import fs2.{Pipe, Stream}
import io.circe._
import io.circe.parser._

object Authorizer extends IOApp {

  def circePipe: Pipe[IO, String, Unit] = _.flatMap { in =>
    val parsingResult = parse(in)
    if (parsingResult.isRight) {
      val s: Seq[Json] = parsingResult.toOption.get.findAllByKey("account")
      if (s.isEmpty) {
        Stream.eval(IO.delay(println("Detected Transaction")))
      } else {
        Stream.eval(IO.delay(println("Detected Account")))
      }
    } else Stream.eval(IO.delay(println("Parsing Failure")))

  }

  override def run(args: List[String]): IO[ExitCode] =
    Stream
      .resource(Blocker[IO])
      .flatMap { blocker =>
        fs2.io
          .stdinUtf8[IO](4096, blocker)
          .through(fs2.text.lines)
          .evalTap(x => IO.delay(println(x)))
          .through(circePipe)

      }
      .compile
      .drain
      .as(ExitCode.Success)
}
