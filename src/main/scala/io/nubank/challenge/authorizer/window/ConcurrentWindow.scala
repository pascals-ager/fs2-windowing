package io.nubank.challenge.authorizer.window

import io.nubank.challenge.authorizer.external.ExternalDomain.Transaction
import monix.eval.Task
import monix.execution.Scheduler
import org.typelevel.log4cats.Logger

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ConcurrentWindow(windowSize: FiniteDuration)(implicit logger: Logger[Task]) {
  var windowMap = new ConcurrentHashMap[String, mutable.ListBuffer[(Long, Long)]]()

  def clearWindow(): Task[Unit] = Task.eval(windowMap.clear())

  def getWindow(merchant: String, amount: Int): Task[mutable.Seq[(Long, Long)]] = {
    Task.eval(windowMap.get(merchant + amount.toString))
  }

  def putWindow(transaction: Transaction): Task[ListBuffer[(Long, Long)]] =
    Task.eval {
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

  def getWindowSize: Task[Int] = Task.eval {
    var size = 0
    for (transactionTimeEntries <- windowMap.values().asScala) {
      size += transactionTimeEntries.size
    }
    size
  }

  def evictionFunction(implicit scheduler: Scheduler): Runnable = new Runnable {
    override def run(): Unit = {
      try {
        logger.info(s"Running cache eviction").runToFuture
        for (entries <- windowMap.entrySet().asScala) {
          val entryKey: String                   = entries.getKey
          val entryVal: ListBuffer[(Long, Long)] = entries.getValue
          val curr: Long                         = System.currentTimeMillis()

          if (curr - entryVal.reverse.head._2 >= windowSize.toMillis) {
            logger.info(s"Removing expired cache key ${entryKey}").runToFuture
            windowMap.remove(entryKey)
          } else {
            entryVal.filterInPlace(item => (curr - item._2) <= windowSize.toMillis)
          }

        }
      } catch {
        case ex: Exception =>
          logger.error(s"Let's catch all errors, can't throw during eviction!- ${ex}")
      }
    }
  }
}
