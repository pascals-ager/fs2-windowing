package io.win.stream.authorizer.events

import cats.effect.concurrent.{Deferred, Semaphore}
import cats.effect.{ContextShift, IO, Timer}
import fs2.Stream
import fs2.concurrent.Topic
import io.circe.DecodingFailure
import io.win.stream.authorizer.external.ExternalDomain
import io.win.stream.authorizer.external.ExternalDomain.{
  Account,
  AccountEvent,
  AccountState,
  ExternalEvent,
  Start,
  Transaction,
  TransactionEvent
}
import io.win.stream.authorizer.stores.AccountStoreService
import io.win.stream.authorizer.validations.`card-not-active`
import io.win.stream.authorizer.window.TransactionWindow
import io.win.stream.authorizer.window.TransactionWindow.acquireWindow
import monix.execution.Cancelable
import org.scalatest.funspec.AnyFunSpec
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration.DurationInt
import scala.util.Random

class AsyncEventsProcessorSpec extends AnyFunSpec {

  private implicit val cs: ContextShift[IO]          = IO.contextShift(global)
  private implicit val timer: Timer[IO]              = IO.timer(global)
  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val storeRes: Stream[IO, AccountStoreService]      = Stream.resource(AccountStoreService.create())
  /* Increase the frequent and doubled transaction so as not to trigger these violations errors */
  val windowRes: Stream[IO, (TransactionWindow, Cancelable)] = Stream.resource(acquireWindow(2.minutes, 1000, 100))

  /* Randomized Invalidate AccountEvent */
  val acctInvalidate: Stream[IO, Either[DecodingFailure, ExternalEvent]] = for {
    _       <- Stream.eval(IO.delay(Thread.sleep(Random.between(1, 10))))
    invalid <- Stream.eval(IO.pure(Right(AccountEvent(Account(false, 1500)))))
  } yield invalid

  val inputStream: Stream[IO, Either[DecodingFailure, ExternalEvent]] =
    Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 60, 1581256223, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 40, 1581256224, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Apple", 100, 1581256225, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 60, 1581256226, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 40, 1581256227, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Apple", 100, 1581256228, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 60, 1581256229, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 40, 1581256230, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Apple", 100, 1581256231, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 60, 1581256232, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 40, 1581256233, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Apple", 100, 1581256234, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 60, 1581256235, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 40, 1581256236, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Apple", 100, 1581256237, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 60, 1581256238, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 40, 1581256239, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Apple", 100, 1581256240, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 60, 1581256241, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Nike", 40, 1581256242, System.currentTimeMillis()))))) ++
      Stream.eval(IO.pure(Right(TransactionEvent(Transaction("Apple", 100, 1581256243, System.currentTimeMillis())))))

  it("Async TransactionEvents should produce the same AccountState") {

    val topicStream = Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))

    val publishStream = storeRes.flatMap { store =>
      windowRes.flatMap { window =>
        val events = for {
          semaphore <- Stream.eval(Semaphore[IO](1))
          _         <- Stream.eval(store.putAccount(Account(true, 1500)))
          topic     <- topicStream
          eventsService = new EventsProcessor(store, window._1, topic, semaphore)

          events <- inputStream
            .mapAsyncUnordered(8)(transaction => IO.pure(transaction)) /* change ordering of streams */
            .balanceThrough(1, 8)(eventsService.authorizeEvents(semaphore))
          /* send one event at a time balanced through 8 authorizers */
        } yield events
        events.compile.drain.unsafeRunSync()
        for {
          latest <- Stream.eval(store.getAccount())
        } yield latest
      }
    }

    for (loop <- 1 to 1000) {
      val finalAcctList: List[Option[Account]] = publishStream.compile.toList.unsafeRunSync()
      val finalAcctState                       = finalAcctList.head
      assert(
        finalAcctState.contains(Account(true, 100))
      )
    }
  }

  it("Async TransactionEvents should produce expected AccountState with invalidate account events") {

    val topicStream = Stream.eval(Topic[IO, Either[DecodingFailure, ExternalEvent]](Right(Start)))

    val publishStream = storeRes.flatMap { store =>
      windowRes.flatMap { window =>
        val events = for {
          semaphore <- Stream.eval(Semaphore[IO](1))
          _         <- Stream.eval(store.putAccount(Account(true, 1500)))
          topic     <- topicStream
          eventsService = new EventsProcessor(store, window._1, topic, semaphore)

          events <- inputStream
            .mapAsyncUnordered(8)(transaction => IO.pure(transaction))
            .balanceThrough(1, 8)(eventsService.authorizeEvents(semaphore))
            .concurrently(acctInvalidate.through(eventsService.authorizeEvents(semaphore)))

        } yield events
        events.compile.drain.unsafeRunSync()
        for {
          latest <- Stream.eval(store.getAccount())
        } yield latest
      }
    }

    for (loop <- 1 to 1000) {
      val finalAcctList: List[Option[Account]] = publishStream.compile.toList.unsafeRunSync()
      val finalAcctState                       = finalAcctList.head
      assert(
        finalAcctState.contains(Account(true, 100)) || finalAcctState.contains(Account(false, 1500))
      )
    }
  }

}
