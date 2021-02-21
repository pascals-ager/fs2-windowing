package io.nubank.challenge.authorizer.events

import cats.effect.concurrent.{Deferred, Semaphore}
import cats.effect.{ContextShift, IO, Timer}
import fs2.{Pure, Stream}
import fs2.concurrent.Topic
import io.circe.DecodingFailure
import io.nubank.challenge.authorizer.external.ExternalDomain
import io.nubank.challenge.authorizer.external.ExternalDomain.{
  Account,
  AccountEvent,
  AccountState,
  ExternalEvent,
  Start,
  Transaction,
  TransactionEvent
}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.window.TransactionWindow
import io.nubank.challenge.authorizer.window.TransactionWindow.acquireWindow
import monix.execution.Cancelable
import org.scalatest.funspec.AnyFunSpec
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration.DurationInt

class EventsProcessorSpec extends AnyFunSpec {
  private implicit val cs: ContextShift[IO]          = IO.contextShift(global)
  private implicit val timer: Timer[IO]              = IO.timer(global)
  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  val eventOne   = """{"account": {"active-card": true, "available-limit": 100}}"""
  val eventTwo   = """{"transaction": {"merchant": "Burger King", "amount": 20, "time": "2019-02-13T10:00:00.000Z"}}"""
  val eventThree = """{"transaction": {"merchant": "Habbib's", "amount": 90, "time": "2019-02-13T11:00:00.000Z"}}"""

  val storeRes: Stream[IO, AccountStoreService]              = Stream.resource(AccountStoreService.create())
  val windowRes: Stream[IO, (TransactionWindow, Cancelable)] = Stream.resource(acquireWindow(2.minutes))

  it("Events Classification should encode AccountEvent correctly") {

    val inputStream = Stream(eventOne)
    val topicStream = Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))

    val producerStream: Stream[IO, Either[DecodingFailure, ExternalEvent]] = storeRes.flatMap { store =>
      windowRes.flatMap { window =>
        for {
          semaphore <- Stream.eval(Semaphore[IO](1))
          topic     <- topicStream
          eventsService = new EventsProcessor(store, window._1, topic, semaphore)
          testStream <- inputStream.through(eventsService.eventsClassificationPipe)
        } yield testStream
      }
    }

    val test = producerStream.compile.toList.unsafeToFuture().value.get.toOption.get
    assert(test.length == 1)
    assert(test.head == Right(AccountEvent(Account(true, 100))))

  }

  it("Events Classification should encode and enrich TransactionEvent correctly") {

    val inputStream = Stream(eventTwo)
    val topicStream = Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))

    val producerStream: Stream[IO, Either[DecodingFailure, ExternalEvent]] = storeRes.flatMap { store =>
      windowRes.flatMap { window =>
        for {
          semaphore <- Stream.eval(Semaphore[IO](1))
          topic     <- topicStream
          eventsService = new EventsProcessor(store, window._1, topic, semaphore)
          testStream <- inputStream.through(eventsService.eventsClassificationPipe)
        } yield testStream
      }
    }

    val test = producerStream.compile.toList.unsafeToFuture().value.get.toOption.get
    assert(test.length == 1)
    assert(test.head.isRight)

    test.head match {
      case Right(value) =>
        value match {
          case event: TransactionEvent => {
            assert(event.transaction.merchant == "Burger King")
            assert(event.transaction.amount == 20)
            assert(
              event.transaction.transactionTime == OffsetDateTime
                .parse("2019-02-13T10:00:00.000Z", DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .toEpochSecond
            )
            assert(
              event.transaction.processingTime.isValidLong
            ) /* can't assert this easily cos enriching uses timestamp during encoding*/
          }
          case event: AccountEvent => assert(false) /* ToDo: can be improved. But works */
          case Start               => assert(false) /* breaker */
        }
      case Left(ex) => assert(false) /* breaker */
    }
  }

  it("Events publish and subscribe to topic") {
    val inputStream: Stream[IO, Either[DecodingFailure, ExternalEvent]] =
      Stream.eval(IO.pure(Right(AccountEvent(Account(true, 40)))))
    val topicStream = Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))

    val publishStream: Stream[IO, Either[DecodingFailure, ExternalEvent]] = storeRes.flatMap { store =>
      windowRes.flatMap { window =>
        for {
          semaphore <- Stream.eval(Semaphore[IO](1))
          topic     <- topicStream
          eventsService = new EventsProcessor(store, window._1, topic, semaphore)
          testStream <- inputStream.through(eventsService.eventsPublishPipe)
          events     <- eventsService.eventsSubscriber
        } yield events
      }
    }

    val event: List[Either[DecodingFailure, ExternalEvent]] = publishStream.take(1).compile.toList.unsafeRunSync()

    assert(event.head == Right(AccountEvent(Account(true, 40))))
  }

  it("Authorize AccountEvent from topic") {
    val inputStream: Stream[IO, Either[DecodingFailure, ExternalEvent]] =
      Stream.eval(IO.pure(Right(AccountEvent(Account(true, 40)))))
    val topicStream = Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))

    val publishStream: Stream[IO, Option[ExternalDomain.AccountState]] = storeRes.flatMap { store =>
      windowRes.flatMap { window =>
        for {
          semaphore <- Stream.eval(Semaphore[IO](1))
          //_     <- Stream.eval(store.putAccount(Account(true, 100)))
          topic <- topicStream
          eventsService = new EventsProcessor(store, window._1, topic, semaphore)
          testStream <- inputStream.through(eventsService.eventsPublishPipe)
          events     <- eventsService.eventsSubscriber.through(eventsService.authorizeEvents(semaphore))
        } yield events
      }
    }

    val event: List[Option[ExternalDomain.AccountState]] = publishStream.take(1).compile.toList.unsafeRunSync()

    assert(event.head.get == AccountState(Some(Account(true, 40)), List()))
  }

  it("Authorize TransactionEvent from topic") {
    val inputStream: Stream[IO, Either[DecodingFailure, ExternalEvent]] =
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 60, 1581256223, System.currentTimeMillis())))))
    val topicStream = Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))

    val publishStream: Stream[IO, Option[ExternalDomain.AccountState]] = storeRes.flatMap { store =>
      windowRes.flatMap { window =>
        for {
          semaphore <- Stream.eval(Semaphore[IO](1))
          _         <- Stream.eval(store.putAccount(Account(true, 100)))
          topic     <- topicStream
          eventsService = new EventsProcessor(store, window._1, topic, semaphore)
          testStream <- inputStream.through(eventsService.eventsPublishPipe)
          events     <- eventsService.eventsSubscriber.through(eventsService.authorizeEvents(semaphore))
        } yield events
      }
    }

    val event: List[Option[ExternalDomain.AccountState]] = publishStream.take(1).compile.toList.unsafeRunSync()

    assert(event.head.get == AccountState(Some(Account(true, 40)), List()))
  }
}
