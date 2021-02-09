package io.nubank.challenge.authorizer.window

import cats.effect.{IO, Resource, Timer}
import cats.data.Validated
import cats.effect.concurrent.Ref
import scalacache.{Entry, Mode}
import scalacache.guava._
import com.google.common.cache.{Cache, CacheBuilder}
import io.nubank.challenge.authorizer.external.ExternalDomain.Transaction
import io.nubank.challenge.authorizer.validations.DomainValidation

import java.time.Clock
import java.util.concurrent.{ConcurrentMap, TimeUnit}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

trait TransactionWindow {
  def get(merchant: String, amount: Int): IO[Option[Seq[(Long, Long)]]]
  def put(transaction: Transaction): IO[Unit]
  def getSize: IO[Long]
  def evictExpiredTimestamps: IO[Unit]
}

object TransactionWindow {

  def create(expirationSeconds: FiniteDuration): Resource[IO, TransactionWindow] = {
    implicit val mode: Mode[IO] = scalacache.CatsEffect.modes.async
    implicit val clock: Clock   = Clock.systemUTC()

    val build: IO[Ref[IO, GuavaCache[ListBuffer[(Long, Long)]]]] = Ref[IO].of(
      new GuavaCache(
        CacheBuilder
          .newBuilder()
          .expireAfterWrite(expirationSeconds._1, expirationSeconds._2)
          .maximumSize(100L)
          .build[String, Entry[ListBuffer[(Long, Long)]]]
      )
    )

    Resource
      .makeCase(build) {
        case (window, _) =>
          IO.delay {
            window.get.map(win => {
              println("cleaning up underlying cache")
              win.underlying.cleanUp()
            })
          }
      }
      .map { window =>
        new TransactionWindow {

          override def put(transaction: Transaction): IO[Unit] =
            for {
              mod <- window.update(win =>
                Option(win.underlying.getIfPresent(transaction.merchant + transaction.amount.toString)) match {
                  case Some(entry) =>
                    entry.value += Tuple2(transaction.transactionTime, transaction.processingTime)
                    win
                  case None =>
                    win.underlying.put(
                      transaction.merchant + transaction.amount.toString,
                      Entry(ListBuffer((transaction.transactionTime, transaction.processingTime)), None)
                    )
                    win
                }
              )
            } yield mod

          /*IO.delay {
            Option(window.underlying.getIfPresent(transaction.merchant + transaction.amount.toString)) match {
              case Some(entry) =>
                entry.value += (transaction.transactionTime, transaction.processingTime)
              case None =>
                window.underlying.put(transaction.merchant + transaction.amount.toString, Entry(ListBuffer( (transaction.transactionTime, transaction.processingTime)), None )  )
            }
          }*/

          override def getSize: IO[Long] =
            for {
              win <- window.get
              size <- IO.delay {
                win.underlying.size()
              }
            } yield size

          override def get(merchant: String, amount: Int): IO[Option[Seq[(Long, Long)]]] =
            for {
              win <- window.get
              item <- IO.delay {
                Option(win.underlying.getIfPresent(merchant + amount.toString)) match {
                  case Some(entry) => Some(entry.value.toSeq)
                  case None        => None
                }
              }
            } yield item

          override def evictExpiredTimestamps: IO[Unit] =
            for {
              mod <- window.update(win => {
                for ((key, entry) <- win.underlying.asMap().asScala) {
                  entry.value.filter(item => (System.currentTimeMillis() - item._2) >= 120000)
                }
                win
              })
              _ <- IO.delay(println("Running evictExpiredTimestamps"))
            } yield mod

        }
      }
  }
}
