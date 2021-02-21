package io.nubank.challenge.authorizer

import cats.effect.concurrent.{Deferred, Semaphore}
import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import fs2.concurrent.{SignallingRef, Topic}
import io.circe._
import io.nubank.challenge.authorizer.configs.createStreamsProps
import io.nubank.challenge.authorizer.events.EventsProcessor
import io.nubank.challenge.authorizer.exception.DomainException.{
  DecodingFailureException,
  ParsingFailureException,
  UnrecognizedEventException
}
import io.nubank.challenge.authorizer.external.ExternalDomain.{ExternalEvent, Start}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.window.TransactionWindow.acquireWindow
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.Properties
import scala.concurrent.duration.DurationInt

object Authorizer extends IOApp {

  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  private val streamProps: IO[Properties]            = IO.pure(createStreamsProps)

  override def run(args: List[String]): IO[ExitCode] = {

    val outer: Stream[IO, Unit] = for {
      props       <- Stream.eval(streamProps)
      interrupter <- Stream.eval(SignallingRef[IO, Boolean](false))
      topic       <- Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))
      semaphore   <- Stream.eval(Semaphore[IO](1))
      store       <- Stream.resource(AccountStoreService.create())
      window <- Stream.resource(
        acquireWindow(
          props.getProperty("TIME_WINDOW_SIZE_SECONDS").toInt.seconds,
          props.getProperty("TRANSACTION_FREQUENCY_TOLERANCE").toInt,
          props.getProperty("TRANSACTION_DOUBLED_TOLERANCE").toInt
        )
      )
      authService = new EventsProcessor(store, window._1, topic, semaphore)
      _ <- authService.eventsHandler
        .concurrently(authService.consumeEvents)
        .interruptWhen(interrupter)
    } yield ()

    outer
      .handleErrorWith {
        case unrecognized: UnrecognizedEventException => Stream.eval(logger.warn(unrecognized.msg))
        case parsing: ParsingFailureException         => Stream.eval(logger.warn(parsing.msg))
        case decoding: DecodingFailureException       => Stream.eval(logger.warn(decoding.msg))
        case throwable: Throwable                     => Stream.eval(logger.warn(s"Unknown exception occurred:  ${throwable.getMessage}"))
      }
      .compile
      .drain
      .as(ExitCode.Success)
  }

}
