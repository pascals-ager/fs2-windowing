package io.nubank.challenge.authorizer.window

import cats.effect.IO
import cats.effect.concurrent.Ref
import io.nubank.challenge.authorizer.external.ExternalDomain.Transaction
import scalacache.{Entry, Mode}
import scalacache.guava.GuavaCache

import java.time.Clock
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

class TransactionWindow(window: Ref[IO, GuavaCache[ListBuffer[(Long, Long)]]])(implicit mode: Mode[IO], clock: Clock) {

  def put(transaction: Transaction): IO[Unit] =
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

  def getSize: IO[Long] =
    for {
      win <- window.get
      size <- IO.delay {
        win.underlying.size()
      }
    } yield size

  def get(merchant: String, amount: Int): IO[Option[Seq[(Long, Long)]]] =
    for {
      win <- window.get
      item <- IO.delay {
        Option(win.underlying.getIfPresent(merchant + amount.toString)) match {
          case Some(entry) => Some(entry.value.toSeq)
          case None        => None
        }
      }
    } yield item

  def evictExpiredTimestamps(timestampEvictionInterval: FiniteDuration): IO[Unit] =
    for {
      curr <- IO.delay(System.currentTimeMillis())
      _    <- IO.delay(println(s"Running evictExpiredTimestamps at ${curr}"))
      mod <- window.update(win => {
        for ((key, entry) <- win.underlying.asMap().asScala) {
          entry.value.filterInPlace(item => (curr - item._2) <= timestampEvictionInterval.toMillis)
        }
        win
      })
    } yield mod

}
