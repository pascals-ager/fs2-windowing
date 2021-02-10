package io.nubank.challenge.authorizer.window

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.Ref
import org.scalatest.funspec.AnyFunSpec
import fs2._
import _root_.io.nubank.challenge.authorizer.external.ExternalDomain.Transaction
import cats.implicits._
import com.google.common.cache.CacheBuilder
import org.scalatest.Assertion
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalacache.{Entry, Mode}
import scalacache.guava.GuavaCache

import java.time.Clock
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class TransactionWindowSpec extends AnyFunSpec {

  implicit val timer: Timer[IO]                      = IO.timer(ExecutionContext.global)
  implicit val cs: ContextShift[IO]                  = IO.contextShift(ExecutionContext.global)
  implicit val mode: Mode[IO]                        = scalacache.CatsEffect.modes.async
  implicit val clock: Clock                          = Clock.systemUTC()
  val cacheExpirationInterval: FiniteDuration        = 2.minutes
  val timestampEvictionInterval: FiniteDuration      = 10.seconds
  implicit def logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  val build: IO[Ref[IO, GuavaCache[ListBuffer[(Long, Long)]]]] = Ref[IO].of(
    new GuavaCache(
      CacheBuilder
        .newBuilder()
        .expireAfterWrite(cacheExpirationInterval._1, cacheExpirationInterval._2)
        .maximumSize(100L)
        .build[String, Entry[ListBuffer[(Long, Long)]]]
    )
  )

  val res: Resource[IO, Ref[IO, GuavaCache[ListBuffer[(Long, Long)]]]] = Resource
    .makeCase(build) {
      case (window, _) =>
        IO.delay {
          window.get.map(win => {
            logger.info("Cleaning up underlying cache")
            win.underlying.cleanUp()
          })
        }
    }

  it("The second entry should be expired") {

    val timestampExpirationStream: Stream[IO, Option[Seq[(Long, Long)]]] = Stream.resource(res).flatMap { cache =>
      val win: TransactionWindow = new TransactionWindow(cache)

      val step: Stream[IO, Option[Seq[(Long, Long)]]] = Stream.eval(for {
        tsOne          <- IO.pure(System.currentTimeMillis())
        _              <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne <- IO.pure(Transaction("Nike", 240, 1581256283, tsOne))
        putOne         <- win.put(transactionOne)
        _              <- logger.info(s"First Transaction Success")
        _              <- IO.delay(Thread.sleep(20000))
        tsTwo          <- IO.pure(System.currentTimeMillis())
        _              <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo <- IO.pure(Transaction("Nike", 240, 1581256284, tsTwo))
        putTwo         <- win.put(transactionTwo)
        _              <- logger.info(s"Second Transaction Success")
        get            <- win.get("Nike", 240)
      } yield get)

      val evict = Stream
        .eval(win.evictExpiredTimestamps(timestampEvictionInterval))
        .metered(3.seconds)
        .repeatN(20)

      step
        .concurrently(evict)
    }
    val timestampExpirationTest = for {
      value <- timestampExpirationStream
    } yield (assert(value.get.size == 1 && value.get.head._1 == 1581256284))

    timestampExpirationTest.compile.drain.unsafeRunSync()

  }

}
