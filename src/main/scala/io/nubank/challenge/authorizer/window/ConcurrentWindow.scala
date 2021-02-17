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

class ConcurrentWindow(windowSize: FiniteDuration)(implicit logger: Logger[IO]) {
  val windowMap                  = new ConcurrentHashMap[String, mutable.ListBuffer[(Long, Long)]]()
  lazy val widowSizeMs: IO[Long] = IO.pure(windowSize.toMillis)

  def clearWindow(): IO[Unit] = IO.delay(windowMap.clear())

  def getWindow(merchant: String, amount: Int): IO[Option[mutable.Seq[(Long, Long)]]] = {
    IO.delay(Option(windowMap.get(merchant + amount.toString)))
  }

  def putWindow(transaction: Transaction): IO[ListBuffer[(Long, Long)]] =
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

  def getWindowSize: IO[Int] = IO.delay {
    var size = 0
    for (transactionTimeEntries <- windowMap.values().asScala) {
      size += transactionTimeEntries.size
    }
    size
  }

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

object ConcurrentWindow {
  implicit val cpuScheduler: SchedulerService = Scheduler.computation(1, name = "cpu-bound-eviction-thread")
  def acquireWindow(
      windowSize: FiniteDuration
  )(implicit logger: Logger[IO]): Resource[IO, (ConcurrentWindow, Cancelable)] =
    Resource.make {
      for {
        window   <- IO.delay(new ConcurrentWindow(windowSize))
        runnable <- window.evictionFunction
        evict <- IO.delay(
          cpuScheduler.scheduleWithFixedDelay(
            windowSize._1,
            windowSize._1,
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
