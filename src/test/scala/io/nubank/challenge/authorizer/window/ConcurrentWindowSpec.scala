package io.nubank.challenge.authorizer.window

import cats.effect.{IO, Resource}
import io.nubank.challenge.authorizer.external.ExternalDomain.Transaction
import monix.eval.Task
import monix.execution.{Cancelable, CancelableFuture, Scheduler}

import scala.concurrent.duration._
import org.scalatest.funspec.AnyFunSpec
import monix.execution.schedulers.{AsyncScheduler, SchedulerService, TestScheduler}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.Assertion
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.collection.mutable
import scala.util.Try

class ConcurrentWindowSpec extends AnyFunSpec {
  implicit def logger: SelfAwareStructuredLogger[Task] = Slf4jLogger.getLogger[Task]
  val cpuScheduler: SchedulerService                   = Scheduler.computation(1, name = "cpu-bound-eviction-thread")

  def acquireWindow(windowSize: FiniteDuration): Resource[Task, (ConcurrentWindow, Cancelable)] =
    Resource.make {
      Task.eval {
        val window: ConcurrentWindow = new ConcurrentWindow(windowSize)
        val evictor: Cancelable =
          cpuScheduler.scheduleWithFixedDelay(
            windowSize._1 / 2,
            windowSize._1 / 3,
            windowSize._2,
            window.evictionFunction(cpuScheduler)
          )
        Tuple2(window, evictor)
      }
    } { res =>
      for {
        _ <- logger.info("Shutting down eviction thread")
        _ <- Task.eval(res._2.cancel())
        _ <- logger.info("Clearing ConcurrentWindow")
        _ <- Task.eval(res._1.clearWindow())
      } yield ()
    }

  it("Write and read two entries") {
    val test: Task[Vector[(Long, Long)]] = acquireWindow(2.minutes).use { window =>
      val currTs = Task.eval(System.currentTimeMillis())
      for {
        tsOne          <- currTs
        _              <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne <- Task.pure(Transaction("Nike", 240, 1581256213, tsOne))
        _              <- window._1.putWindow(transactionOne)
        _              <- logger.info(s"First Transaction Success")
        tsTwo          <- currTs
        _              <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo <- Task.pure(Transaction("Addidas", 220, 1581256214, tsTwo))
        _              <- window._1.putWindow(transactionTwo)
        _              <- logger.info(s"Second Transaction Success")
        entryOne       <- window._1.getWindow("Nike", 240)
        entryTwo       <- window._1.getWindow("Addidas", 220)
      } yield Vector(entryOne, entryTwo).flatten
    }
    val testOutputVector: Vector[(Long, Long)] = test.runToFuture.value.get.toOption.get

    assert(testOutputVector(0)._1 == 1581256213)
    assert(testOutputVector(1)._1 == 1581256214)
  }

  it("Write and read two entries with same key") {
    val test: Task[mutable.Seq[(Long, Long)]] = acquireWindow(20.seconds).use { window =>
      val currTs = Task.eval(System.currentTimeMillis())
      for {
        tsOne          <- currTs
        _              <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne <- Task.pure(Transaction("Nike", 240, 1581256223, tsOne))
        _              <- window._1.putWindow(transactionOne)
        _              <- logger.info(s"First Transaction Success")
        tsTwo          <- currTs
        _              <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo <- Task.pure(Transaction("Nike", 240, 1581256224, tsTwo))
        _              <- window._1.putWindow(transactionTwo)
        _              <- logger.info(s"Second Transaction Success")
        entryOne       <- window._1.getWindow("Nike", 240)
      } yield entryOne
    }
    val testOutputSeq: mutable.Seq[(Long, Long)] = test.runToFuture.value.get.toOption.get
    assert(testOutputSeq.size == 2)
    assert(testOutputSeq.head._1 == 1581256223)
    assert(testOutputSeq(1)._1 == 1581256224)
  }

  it("Older entry of the same key should expire") {
    val test: Task[mutable.Seq[(Long, Long)]] = acquireWindow(20.seconds).use { window =>
      val currTs = Task.eval(System.currentTimeMillis())
      for {
        tsOne          <- currTs
        _              <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne <- Task.pure(Transaction("Nike", 240, 1581256233, tsOne))
        _              <- window._1.putWindow(transactionOne)
        _              <- logger.info(s"First Transaction Success")
        _              <- Task.eval(Thread.sleep(10000))
        tsTwo          <- currTs
        _              <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo <- Task.pure(Transaction("Nike", 240, 1581256234, tsTwo))
        _              <- window._1.putWindow(transactionTwo)
        _              <- logger.info(s"Second Transaction Success")
        _              <- Task.eval(Thread.sleep(12000))
        entryOne       <- window._1.getWindow("Nike", 240)
      } yield entryOne
    }
    val testOutputSeq: mutable.Seq[(Long, Long)] = test.runToFuture.value.get.toOption.get
    assert(testOutputSeq.size == 1)
    assert(testOutputSeq.head._1 == 1581256234)
  }

  it("Size of window with distinct key transactions") {
    val test: Task[Int] = acquireWindow(20.seconds).use { window =>
      val currTs = Task.eval(System.currentTimeMillis())
      for {
        tsOne            <- currTs
        _                <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne   <- Task.pure(Transaction("Nike", 240, 1581256263, tsOne))
        _                <- window._1.putWindow(transactionOne)
        _                <- logger.info(s"First Transaction Success")
        tsTwo            <- currTs
        _                <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo   <- Task.pure(Transaction("Addidas", 240, 1581256264, tsTwo))
        _                <- window._1.putWindow(transactionTwo)
        tsThree          <- currTs
        _                <- logger.info(s"Using ${tsThree} for third transaction ")
        transactionThree <- Task.pure(Transaction("Puma", 240, 1581256265, tsThree))
        _                <- window._1.putWindow(transactionThree)
        _                <- logger.info(s"Third Transaction Success")
        size             <- window._1.getWindowSize
      } yield size
    }
    val testSize: Int = test.runToFuture.value.get.toOption.get
    assert(testSize == 3)
  }

  it("Size of window with cache expiration and distinct key transactions") {
    val test = acquireWindow(20.seconds).use { window =>
      val currTs = Task.eval(System.currentTimeMillis())
      for {
        tsOne            <- currTs
        _                <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne   <- Task.pure(Transaction("Nike", 240, 1581256263, tsOne))
        _                <- window._1.putWindow(transactionOne)
        _                <- logger.info(s"First Transaction Success")
        tsTwo            <- currTs
        _                <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo   <- Task.pure(Transaction("Addidas", 240, 1581256264, tsTwo))
        _                <- window._1.putWindow(transactionTwo)
        _                <- Task.delay(Thread.sleep(60000))
        tsThree          <- currTs
        _                <- logger.info(s"Using ${tsThree} for third transaction ")
        transactionThree <- Task.pure(Transaction("Puma", 240, 1581256265, tsThree))
        _                <- window._1.putWindow(transactionThree)
        _                <- logger.info(s"Third Transaction Success")
        size             <- window._1.getWindowSize
      } yield size
    }
    val testSize: Int = test.runToFuture.value.get.toOption.get
    assert(testSize == 1)
  }

  it("Size of window with entry expiration and multi-key transactions") {
    val test = acquireWindow(20.seconds).use { window =>
      val currTs = Task.eval(System.currentTimeMillis())
      for {
        tsOne            <- currTs
        _                <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne   <- Task.pure(Transaction("Nike", 240, 1581256263, tsOne))
        _                <- window._1.putWindow(transactionOne)
        _                <- logger.info(s"First Transaction Success")
        _                <- Task.delay(Thread.sleep(30000))
        tsTwo            <- currTs
        _                <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo   <- Task.pure(Transaction("Nike", 240, 1581256264, tsTwo))
        _                <- window._1.putWindow(transactionTwo)
        tsThree          <- currTs
        _                <- logger.info(s"Using ${tsThree} for third transaction ")
        transactionThree <- Task.pure(Transaction("Nike", 240, 1581256265, tsThree))
        _                <- window._1.putWindow(transactionThree)
        _                <- logger.info(s"Third Transaction Success")
        size             <- window._1.getWindowSize
      } yield size
    }
    val testSize: Int = test.runToFuture.value.get.toOption.get
    assert(testSize == 2)
  }

}
