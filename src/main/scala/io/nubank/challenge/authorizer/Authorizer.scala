package io.nubank.challenge.authorizer

import cats.data.ValidatedNec
import cats.effect.{Blocker, ContextShift, ExitCode, IO, IOApp, Resource}
import fs2.{Pipe, Stream}
import cats.effect.implicits._
import cats.implicits.catsSyntaxTuple4Semigroupal
import fs2.concurrent.{SignallingRef, Topic}
import io.circe.Decoder.Result
import io.circe._
import io.circe.parser._
import io.circe.syntax.EncoderOps
import io.nubank.challenge.authorizer.exception.DomainException.{
  DecodingFailureException,
  ParsingFailureException,
  UnrecognizedEventType
}
import io.nubank.challenge.authorizer.external.ExternalDomain.{
  Account,
  AccountEvent,
  AccountState,
  ExternalEvent,
  Start,
  Transaction,
  TransactionEvent
}
import io.nubank.challenge.authorizer.stores.AccountStore
import io.nubank.challenge.authorizer.validations.DomainValidation
import io.nubank.challenge.authorizer.window.ConcurrentWindow
import io.nubank.challenge.authorizer.window.ConcurrentWindow.acquireWindow
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

object Authorizer extends IOApp {

  implicit def logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  /*  val topic: Stream[IO, Topic[IO, Either[DecodingFailure, ExternalEvent]]] =
    Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))*/

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

    def authThis(
        topic: Topic[IO, Either[DecodingFailure, ExternalEvent]]
    )(implicit store: AccountStore, window: ConcurrentWindow): Stream[IO, Unit] = {

      val events = topic.subscribe(10)
      def authorizeTransactions: Pipe[IO, Either[DecodingFailure, ExternalEvent], Unit] = _.flatMap {
        case Left(ex) => Stream.raiseError[IO](DecodingFailureException(ex.message))
        case Right(value) =>
          value match {
            case AccountEvent(account) => {
              val s: Stream[IO, AccountState] =
                for {
                  _     <- Stream.eval(IO.delay(println(s"Received AccountEvent: ${account}")))
                  valid <- Stream.eval(DomainValidation.validateAccount(account)(store))
                  _ <- valid.toEither match {
                    case Right(acc) => Stream.eval(store.put(acc))
                    case Left(err)  => Stream.emit(())
                  }
                  domainValidation <- Stream.eval(valid.fold(l => IO.delay(l.toChain.toList), r => IO.delay(List())))
                } yield AccountState(Some(account), domainValidation)

              s.evalMap(item => IO.delay(println(item)))
            }

            case TransactionEvent(transaction) =>
              for {

                firstValidation  <- Stream.eval(DomainValidation.validatedAccountActive(transaction)(store))
                secondValidation <- Stream.eval(DomainValidation.validatedAccountBalance(transaction)(store))
                thirdValidation  <- Stream.eval(DomainValidation.validatedTransactionFrequency(transaction)(window))
                fourthValidation <- Stream.eval(DomainValidation.validatedDoubledTransaction(transaction)(window))
                violations <- Stream.eval {
                  for {
                    firstViolation  <- firstValidation.fold(l => IO.delay(l.toChain.toList), r => IO.delay(List()))
                    secondViolation <- secondValidation.fold(l => IO.delay(l.toChain.toList), r => IO.delay(List()))
                    thirdViolation  <- thirdValidation.fold(l => IO.delay(l.toChain.toList), r => IO.delay(List()))
                    fourthViolation <- fourthValidation.fold(l => IO.delay(l.toChain.toList), r => IO.delay(List()))
                  } yield firstViolation ++ secondViolation ++ thirdViolation ++ fourthViolation
                }
                out <- if (firstValidation.isValid && secondValidation.isValid && thirdValidation.isValid && fourthValidation.isValid) {
                  for {
                    _        <- Stream.eval(window.putWindow(transaction))
                    oldState <- Stream.eval(store.get())
                    put <- Stream.eval(
                      store
                        .put(Account(oldState.get.`active-card`, oldState.get.`available-limit` - transaction.amount))
                    )
                    newState <- Stream.eval(store.get())
                    print    <- Stream.eval(IO.delay(println(AccountState(newState, List()))))
                  } yield print
                } else {
                  for {
                    oldState <- Stream.eval(store.get())
                    print    <- Stream.eval(IO.delay(println(AccountState(oldState, violations))))
                  } yield print
                }
              } yield out

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
      store       <- Stream.resource(AccountStore.create())
      window      <- Stream.resource(acquireWindow(2.minutes))
      _           <- authThis(topic = topic)(store, window._1).concurrently(readEvents(topic = topic)).interruptWhen(interrupter)
    } yield ()

    outer.compile.drain
      .as(ExitCode.Success)
  }

}
