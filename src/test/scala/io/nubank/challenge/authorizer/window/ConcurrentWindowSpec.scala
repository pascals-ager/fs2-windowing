package io.nubank.challenge.authorizer.window

import cats.effect.{IO, Resource}
import io.nubank.challenge.authorizer.external.ExternalDomain.Transaction
import monix.eval.Task
import monix.execution.{Cancelable, CancelableFuture}

import scala.concurrent.duration._
import org.scalatest.funspec.AnyFunSpec
import monix.execution.schedulers.TestScheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.Assertion
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.collection.mutable
import scala.util.Try

class ConcurrentWindowSpec extends AnyFunSpec {
  val testScheduler: TestScheduler                     = TestScheduler()
  implicit def logger: SelfAwareStructuredLogger[Task] = Slf4jLogger.getLogger[Task]

  def acquireWindow(windowSize: FiniteDuration): Resource[Task, (ConcurrentWindow, Task[Cancelable])] =
    Resource.make {
      Task {
        val window: ConcurrentWindow = new ConcurrentWindow(windowSize)
        val evictor: Task[Cancelable] = Task.eval(
          global.scheduleWithFixedDelay(
            windowSize._1 / 2,
            windowSize._1 / 2,
            windowSize._2,
            window.evictionFunction
          )
        )

        Tuple2(window, evictor)
      }
    } { res =>
      for {
        _           <- Task.eval(println("Closing resources"))
        cancellable <- res._2
        _           <- Task.eval(cancellable.cancel())
        _           <- Task.eval(res._1.clearWindow())
      } yield ()
    }

  it("Write and read two entries") {
    val test: Task[Vector[Try[mutable.Seq[(Long, Long)]]]] = acquireWindow(2.minutes).use { window =>
      for {
        tsOne          <- Task.pure(System.currentTimeMillis())
        _              <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne <- Task.pure(Transaction("Nike", 240, 1581256213, tsOne))
        _              <- Task.eval(window._1.putWindow(transactionOne))
        _              <- logger.info(s"First Transaction Success")
        tsTwo          <- Task.pure(System.currentTimeMillis())
        _              <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo <- Task.pure(Transaction("Addidas", 220, 1581256214, tsTwo))
        _              <- Task.eval(window._1.putWindow(transactionTwo))
        _              <- logger.info(s"Second Transaction Success")
        entryOne       <- Task.eval(window._1.getWindow("Nike", 240))
        entryTwo       <- Task.eval(window._1.getWindow("Addidas", 220))
      } yield Vector(entryOne, entryTwo)
    }
    test.runToFuture.foreach { item =>
      assert(
        item.size == 2 && item(0).toOption.get.size == 1 && item(0).toOption.get.head._1 == 1581256213 &&
          item(1).toOption.get.size == 1 && item(1).toOption.get.head._1 == 1581256214
      )
    }
  }

  it("Write and read two entries with same key") {
    val test: Task[Vector[Try[mutable.Seq[(Long, Long)]]]] = acquireWindow(20.seconds).use { window =>
      for {
        tsOne          <- Task.pure(System.currentTimeMillis())
        _              <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne <- Task.pure(Transaction("Nike", 240, 1581256223, tsOne))
        _              <- Task.eval(window._1.putWindow(transactionOne))
        _              <- logger.info(s"First Transaction Success")
        tsTwo          <- Task.pure(System.currentTimeMillis())
        _              <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo <- Task.pure(Transaction("Nike", 240, 1581256224, tsTwo))
        _              <- Task.eval(window._1.putWindow(transactionTwo))
        _              <- logger.info(s"Second Transaction Success")
        entryOne       <- Task.eval(window._1.getWindow("Nike", 240))
      } yield Vector(entryOne)
    }
    test.runToFuture.foreach { item =>
      assert(
        item.size == 1 && item(0).toOption.get.size == 2 && item(0).toOption.get.head._1 == 1581256223 &&
          item(0).toOption.get(1)._1 == 1581256224
      )
    }
  }

  it("Older entry of the same key should expire") {
    val test: Task[Try[mutable.Seq[(Long, Long)]]] = acquireWindow(20.seconds).use { window =>
      for {
        start          <- window._2.attempt
        tsOne          <- Task.pure(System.currentTimeMillis())
        _              <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne <- Task.pure(Transaction("Nike", 240, 1581256233, tsOne))
        _              <- Task.eval(window._1.putWindow(transactionOne))
        _              <- logger.info(s"First Transaction Success")
        _              <- Task.eval(Thread.sleep(10000))
        tsTwo          <- Task.pure(System.currentTimeMillis())
        _              <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo <- Task.pure(Transaction("Nike", 240, 1581256234, tsTwo))
        _              <- Task.eval(window._1.putWindow(transactionTwo))
        _              <- logger.info(s"Second Transaction Success")
        _              <- Task.eval(Thread.sleep(15000))
        entryOne       <- Task.eval(window._1.getWindow("Nike", 240))
      } yield entryOne

    }
    /*    test
      .runAsync {
        case Left(e)      => println(e)
        case Right(value) => println(s" --> $value")
      }*/
    test.runToFuture.foreach { item => assert(item.toOption.get.size == 1 && item.toOption.get.head._1 == 1581256234) }

  }

}
