package io.nubank.challenge.authorizer.events

import cats.effect.{Blocker, ContextShift, IO, Timer}
import fs2.{Pipe, Stream}
import fs2.concurrent.Topic
import io.circe.DecodingFailure
import io.circe.parser.parse
import io.circe.syntax.EncoderOps
import io.nubank.challenge.authorizer.exception.DomainException.{
  DecodingFailureException,
  ParsingFailureException,
  UnrecognizedEventType
}
import io.nubank.challenge.authorizer.external.ExternalDomain.{AccountEvent, ExternalEvent, Start, TransactionEvent}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.validations.ValidationService
import io.nubank.challenge.authorizer.window.TransactionWindow
import org.typelevel.log4cats.Logger

class EventsProcessor(topic: Topic[IO, Either[DecodingFailure, ExternalEvent]])(
    implicit timer: Timer[IO],
    threadpool: ContextShift[IO],
    logger: Logger[IO]
) {

  def consumeEvents(): Stream[IO, Unit] =
    Stream
      .resource(Blocker[IO])
      .flatMap { blocker =>
        fs2.io
          .stdinUtf8[IO](4096, blocker)
          .repeat
          .through(fs2.text.lines)
          .filter(_.nonEmpty)
          .through(eventsClassificationPipe)
          .through(eventsPublishPipe)
      }

  def eventsClassificationPipe: Pipe[IO, String, Either[DecodingFailure, ExternalEvent]] =
    _.flatMap { in =>
      parse(in) match {
        case Right(value) =>
          if (value.findAllByKey("account").nonEmpty) {
            for {
              _   <- Stream.eval(logger.info("Received AccountEvent: Encoding."))
              enc <- Stream.eval(IO.delay(value.as[AccountEvent]))
            } yield enc
          } else if (value.findAllByKey("transaction").nonEmpty) {
            for {
              _   <- Stream.eval(logger.info("Received TransactionEvent: Encoding with processingTime timestamp"))
              enc <- Stream.eval(IO.delay(value.as[TransactionEvent]))
            } yield enc
          } else {
            Stream.raiseError[IO](
              UnrecognizedEventType("Undefined event received. Expecting account or transaction event types only.")
            )
          }
        case Left(ex) => Stream.raiseError[IO](ParsingFailureException(ex.message))
      }
    }

  def eventsPublishPipe: Pipe[IO, Either[DecodingFailure, ExternalEvent], Unit] =
    _.flatMap { in =>
      for {
        _   <- Stream.eval(logger.info("Received AccountEvent: Publishing to topic."))
        pub <- Stream.eval(topic.publish1(in))
      } yield pub
    }

  def authorizeEvents()(implicit store: AccountStoreService, window: TransactionWindow): Stream[IO, Unit] = {
    val events = topic.subscribe(10)
    def authorizeTransactions: Pipe[IO, Either[DecodingFailure, ExternalEvent], Unit] = _.flatMap {
      case Left(ex) => Stream.raiseError[IO](DecodingFailureException(ex.message))
      case Right(value) =>
        value match {
          case AccountEvent(account) =>
            Stream.eval(ValidationService.validateAndPut(account)).evalMap(item => IO.delay(println(item.asJson)))

          case TransactionEvent(transaction) =>
            Stream.eval(ValidationService.validateAndPut(transaction)).evalMap(item => IO.delay(println(item.asJson)))

          case Start => Stream.emit(())
        }
    }
    events.through(authorizeTransactions)
  }

}
