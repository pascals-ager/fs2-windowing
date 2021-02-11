package io.nubank.challenge.authorizer.window

import io.nubank.challenge.authorizer.external.ExternalDomain.Transaction

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

class ConcurrentWindow(windowSize: FiniteDuration) {
  type MerchantAmountKey = String
  type TransactionTime   = Long
  type ProcessingTime    = Long
  var windowMap = new ConcurrentHashMap[MerchantAmountKey, mutable.ListBuffer[(TransactionTime, ProcessingTime)]]()

  def clearWindow(): Try[Unit] = Try(windowMap.clear())

  def getWindow(merchant: String, amount: Int): Try[mutable.Seq[(TransactionTime, ProcessingTime)]] = {
    Try(windowMap.get(merchant + amount.toString))
  }

  def putWindow(transaction: Transaction): Try[ListBuffer[(TransactionTime, ProcessingTime)]] =
    Try(if (windowMap.containsKey(transaction.merchant + transaction.amount.toString)) {
      var transactionTimeEntries: mutable.ListBuffer[(TransactionTime, ProcessingTime)] =
        windowMap.get(transaction.merchant + transaction.amount.toString)
      transactionTimeEntries += Tuple2(transaction.transactionTime, transaction.processingTime)
      windowMap.replace(transaction.merchant + transaction.amount.toString, transactionTimeEntries)
    } else {
      windowMap.put(
        transaction.merchant + transaction.amount.toString,
        mutable.ListBuffer((transaction.transactionTime, transaction.processingTime))
      )
    })

  def getWindowSize: Try[Int] = Try {
    var size = 0
    for (transactionTimeEntries <- windowMap.values().asScala) {
      size += transactionTimeEntries.size
    }
    size
  }

  def evictionFunction: Runnable = new Runnable {
    override def run(): Unit = {
      try {
        for (entries <- windowMap.entrySet().asScala) {
          val entryKey: MerchantAmountKey                             = entries.getKey
          var entryVal: ListBuffer[(TransactionTime, ProcessingTime)] = entries.getValue
          val curr: TransactionTime                                   = System.currentTimeMillis()

          if (curr - entryVal.reverse.head._2 >= windowSize.toMillis) {
            println(s"removing expired key ${entryKey}")
            windowMap.remove(entryKey)
          } else {
            entryVal.filterInPlace(item => (curr - item._2) <= windowSize.toMillis)
          }

        }
      } catch {
        case ex: Exception => println(s"Let's catch all errors, can't throw!- ${ex}")
      }
    }
  }
}
