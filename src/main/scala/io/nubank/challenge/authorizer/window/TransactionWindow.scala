package io.nubank.challenge.authorizer.window

import cats.effect.{IO, Resource}
import io.nubank.challenge.authorizer.external.ExternalDomain.Transaction
import monix.execution.schedulers.SchedulerService
import monix.execution.{Cancelable, Scheduler}
import org.typelevel.log4cats.Logger

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** ToDo: Implement windows for event time
 *  ToDo: Implement windows for Generic T
 *  TransactionWindow maintains a cache of Transactions for the last 'windowSize' duration.
 *  The implementation uses event processingTime (arrival time) rather than event transaction time (event time).
 *  What that means is that a Transaction with event time: "2019-02-13T10:00:00.000Z" and another with event time:
 * "2019-02-13T11:00:00.000Z" although have one hour between their event times; will belong to the same window of size
 *  2 minutes, if they arrive/are-seen by the streaming system without their natural delay.
 * */
class TransactionWindow(windowSize: FiniteDuration)(implicit logger: Logger[IO]) {
  /* windowMap models the underlying window as HashMap where:
  *  K -> Combination of Transaction merchant and amount.
  *  This can be replaced by any Transaction => String to optimize for common lookup operations which require
  *  O(1) performance. For simpler design the function used for K is Transaction => Transaction.merchant+Transaction.amount
  *  This sufficiently models the use case.
  *  V -> List[(transactionTime, processingTime)]
  *  New transactions for the same key are appended to the list with their timestamp metadata as the value
  *  Eviction -> Evict K entries and Evict `processingTime` timestamp entries in V after `windowSize` expiry period.  *
  * */
  private val windowMap                  = new ConcurrentHashMap[String, mutable.ListBuffer[(Long, Long)]]()
  lazy val widowSizeMs: IO[Long] = IO.pure(windowSize.toMillis)

  /**
   * Clears the Transaction Window in IO
   * @return Unit suspended in IO
   *
   */
  private def clearWindow(): IO[Unit] = IO.delay(windowMap.clear())

  /**
   * @param merchant: The merchant for the transaction
   * @param amount: The amount in the transaction
   * @return The entry for the transaction
   */
  def getTransactionEntry(merchant: String, amount: Int): IO[Option[mutable.Seq[(Long, Long)]]] = {
    IO.delay(Option(windowMap.get(merchant + amount.toString)))
  }

  /**
   * @param transaction: Transaction that must be inserted into the window
   * @return The state of the Transaction entry suspended in IO
   */
  def putTransaction(transaction: Transaction): IO[ListBuffer[(Long, Long)]] =
    IO.delay {
      if (windowMap.containsKey(transaction.merchant + transaction.amount.toString)) {
        var transactionTimeEntries: mutable.ListBuffer[(Long, Long)] =
          windowMap.get(transaction.merchant + transaction.amount.toString)
        transactionTimeEntries += Tuple2(transaction.transactionTime, transaction.processingTime)
        windowMap.replace(transaction.merchant + transaction.amount.toString, transactionTimeEntries)
      } else {
        windowMap.put(
          transaction.merchant + transaction.amount.toString,
          mutable.ListBuffer((transaction.transactionTime, transaction.processingTime))
        )
      }
    }

  /**
   * Size is the total number of transaction entries (V)
   * @return Size of the TransactionWindow
   */
  def getWindowSize: IO[Int] = IO.delay {
    var size = 0
    for (transactionTimeEntries <- windowMap.values().asScala) {
      size += transactionTimeEntries.size
    }
    size
  }

  /**
   * Evict K entries and Evict `processingTime` timestamp entries in V after `windowSize` expiry period.
   * @return A runnable that can be scheduled in the provided scheduler
   */
  def evictionFunction(implicit scheduler: Scheduler): IO[Runnable] = IO.delay {
    new Runnable {
      override def run(): Unit = {
        try {
          logger.info(s"Running cache eviction")
          for (entries <- windowMap.entrySet().asScala) {
            val entryKey: String                   = entries.getKey
            val entryVal: ListBuffer[(Long, Long)] = entries.getValue
            val curr: Long                         = System.currentTimeMillis()

            if (curr - entryVal.reverse.head._2 >= windowSize.toMillis) {
              logger.info(s"Removing expired cache key ${entryKey}")
              windowMap.remove(entryKey)
            } else {
              entryVal.filterInPlace(item => (curr - item._2) <= windowSize.toMillis)
            }

          }
        } catch {
          case ex: Exception =>
            logger.error(s"Let's catch all errors, can't throw during eviction!- ${ex}").unsafeRunSync()
        }
      }
    }
  }
}

object TransactionWindow {
  implicit val cpuScheduler: SchedulerService = Scheduler.computation(1, name = "cpu-bound-eviction-thread")
  def acquireWindow(
      windowSize: FiniteDuration
  )(implicit logger: Logger[IO]): Resource[IO, (TransactionWindow, Cancelable)] =
    Resource.make {
      for {
        window   <- IO.delay(new TransactionWindow(windowSize))
        runnable <- window.evictionFunction
        evict <- IO.delay(
          cpuScheduler.scheduleWithFixedDelay(
            windowSize._1 / 2,
            windowSize._1 / 2,
            windowSize._2,
            runnable
          )
        )
      } yield Tuple2(window, evict)

    } { res =>
      for {
        _ <- logger.info("Shutting down eviction thread")
        _ <- IO.delay(res._2.cancel())
        _ <- logger.info("Clearing ConcurrentWindow")
        _ <- res._1.clearWindow()
      } yield ()
    }
}
