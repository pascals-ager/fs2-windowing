package io.nubank.challenge.authorizer

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import fs2.concurrent.{SignallingRef, Topic}
import io.circe._

import io.nubank.challenge.authorizer.events.EventsProcessor

import io.nubank.challenge.authorizer.external.ExternalDomain.{ExternalEvent, Start}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.window.TransactionWindow.acquireWindow
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

object Authorizer extends IOApp {

  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  override def run(args: List[String]): IO[ExitCode] = {

    val outer: Stream[IO, Unit] = for {
      interrupter <- Stream.eval(SignallingRef[IO, Boolean](false))
      topic       <- Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))
      store       <- Stream.resource(AccountStoreService.create())
      window      <- Stream.resource(acquireWindow(2.minutes))
      authService = new EventsProcessor(topic)
      _ <- authService
        .authorizeEvents()(store, window._1)
        .concurrently(authService.consumeEvents())
        .interruptWhen(interrupter)
    } yield ()

    outer.compile.drain
      .as(ExitCode.Success)
  }

}
