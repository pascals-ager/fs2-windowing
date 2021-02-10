package io.nubank.challenge.authorizer.window

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.Ref
import org.scalatest.funspec.AnyFunSpec
import fs2._
import _root_.io.nubank.challenge.authorizer.external.ExternalDomain.Transaction
import cats.implicits._
import com.google.common.cache.CacheBuilder
import scalacache.{Entry, Mode}
import scalacache.guava.GuavaCache

import java.time.Clock
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class TransactionWindowSpec extends AnyFunSpec {

  implicit val timer: Timer[IO]                 = IO.timer(ExecutionContext.global)
  implicit val cs: ContextShift[IO]             = IO.contextShift(ExecutionContext.global)
  implicit val mode: Mode[IO]                   = scalacache.CatsEffect.modes.async
  implicit val clock: Clock                     = Clock.systemUTC()
  val expirationSeconds: FiniteDuration         = 120.seconds
  val timestampEvictionInterval: FiniteDuration = 10.seconds

  val build: IO[Ref[IO, GuavaCache[ListBuffer[(Long, Long)]]]] = Ref[IO].of(
    new GuavaCache(
      CacheBuilder
        .newBuilder()
        .expireAfterWrite(expirationSeconds._1, expirationSeconds._2)
        .maximumSize(100L)
        .build[String, Entry[ListBuffer[(Long, Long)]]]
    )
  )

  val res: Resource[IO, Ref[IO, GuavaCache[ListBuffer[(Long, Long)]]]] = Resource
    .makeCase(build) {
      case (window, _) =>
        IO.delay {
          window.get.map(win => {
            println("cleaning up underlying cache")
            win.underlying.cleanUp()
          })
        }
    }

  val outer: Stream[IO, Option[Seq[(Long, Long)]]] = Stream.resource(res).flatMap { cache =>
    val win: TransactionWindow = new TransactionWindow(cache)

    val step: IO[Option[Seq[(Long, Long)]]] = for {
      tsOne          <- IO.pure(System.currentTimeMillis())
      _              <- IO.delay(println(s"Using ${tsOne} for first transaction"))
      transactionOne <- IO.pure(Transaction("Nike", 240, 1581256283, tsOne))
      putOne         <- win.put(transactionOne)
      _              <- IO.delay(println(s"First Transaction Success"))
      _              <- IO.delay(Thread.sleep(20000))
      tsTwo          <- IO.pure(System.currentTimeMillis())
      _              <- IO.delay(println(s"Using ${tsTwo} for second transaction "))
      transactionTwo <- IO.pure(Transaction("Nike", 240, 1581256284, tsTwo))
      putTwo         <- win.put(transactionOne)
      _              <- IO.delay(println(s"Second Transaction Success"))
      get            <- win.get("Nike", 240)
    } yield get

    val evict = Stream.eval(win.evictExpiredTimestamps(timestampEvictionInterval)).metered(3.seconds).repeatN(20)
    Stream.eval(step).concurrently(evict)
  }

  outer.evalTap(l => IO.delay(println(l.toString()))).compile.drain.unsafeRunSync()
}
