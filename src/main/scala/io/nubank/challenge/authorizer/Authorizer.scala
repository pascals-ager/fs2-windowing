package io.nubank.challenge.authorizer

import cats.effect.concurrent.Semaphore
import cats.effect.{Blocker, ExitCode, IO, IOApp}
import fs2.{Pipe, Stream}
import fs2.concurrent.{SignallingRef, Topic}
import io.circe._
import io.circe.parser._
import io.nubank.challenge.authorizer.exception.DomainException.{
  DecodingFailureException,
  ParsingFailureException,
  UnrecognizedEventType
}
import io.nubank.challenge.authorizer.external.ExternalDomain.{AccountEvent, ExternalEvent, Start, TransactionEvent}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.validations.ValidationService
import io.nubank.challenge.authorizer.window.ConcurrentWindow
import io.nubank.challenge.authorizer.window.ConcurrentWindow.acquireWindow
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object Authorizer extends IOApp {

  implicit def logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  def eventsClassificationPipe(topic: Topic[IO, Either[DecodingFailure, ExternalEvent]]): Pipe[IO, String, Unit] =
    _.flatMap { in =>
      parse(in) match {
        case Right(value) =>
          if (value.findAllByKey("account").nonEmpty) {
            for {
              pub <- Stream.eval(topic.publish1(value.as[AccountEvent]))
            } yield pub
          } else if (value.findAllByKey("transaction").nonEmpty) {
            for {
              pub <- Stream.eval(topic.publish1(value.as[TransactionEvent]))
            } yield pub
          } else {
            Stream.raiseError[IO](
              UnrecognizedEventType("Undefined event received. Expecting account or transaction event types only.")
            )
          }
        case Left(ex) => Stream.raiseError[IO](ParsingFailureException(ex.message))
      }
    }

  override def run(args: List[String]): IO[ExitCode] = {

    def authorizeEvents(
        semaphore: Semaphore[IO],
        topic: Topic[IO, Either[DecodingFailure, ExternalEvent]]
    )(implicit store: AccountStoreService, window: ConcurrentWindow): Stream[IO, Unit] = {

      val events = topic.subscribe(10)
      def authorizeTransactions: Pipe[IO, Either[DecodingFailure, ExternalEvent], Unit] = _.flatMap {
        case Left(ex) => Stream.raiseError[IO](DecodingFailureException(ex.message))
        case Right(value) =>
          value match {
            case AccountEvent(account) =>
              Stream.eval(ValidationService.validateAndPut(account)).evalMap(item => IO.delay(println(item)))

            case TransactionEvent(transaction) =>
              Stream.eval(ValidationService.validateAndPut(transaction)).evalMap(item => IO.delay(println(item)))

            case Start => Stream.emit(())
          }
      }
      events.through(authorizeTransactions)
    }

    def readEvents(topic: Topic[IO, Either[DecodingFailure, ExternalEvent]]): Stream[IO, Unit] =
      Stream
        .resource(Blocker[IO])
        .flatMap { blocker =>
          fs2.io
            .stdinUtf8[IO](4096, blocker)
            .repeat
            .through(fs2.text.lines)
            .filter(_.nonEmpty)
            .through(eventsClassificationPipe(topic))
        }

    val outer: Stream[IO, Unit] = for {
      interrupter <- Stream.eval(SignallingRef[IO, Boolean](false))
      topic       <- Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))
      sem         <- Stream.eval(Semaphore[IO](1))
      store       <- Stream.resource(AccountStoreService.create())
      window      <- Stream.resource(acquireWindow(2.minutes))
      _ <- authorizeEvents(sem, topic)(store, window._1)
        .concurrently(readEvents(topic))
        .interruptWhen(interrupter)
    } yield ()

    outer.compile.drain
      .as(ExitCode.Success)
  }

}
