package io.nubank.challenge.authorizer.window

import cats.effect.IO
import io.nubank.challenge.authorizer.external.ExternalDomain.Transaction
import io.nubank.challenge.authorizer.window.ConcurrentWindow.acquireWindow

import scala.concurrent.duration._
import org.scalatest.funspec.AnyFunSpec
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scala.collection.mutable

class ConcurrentWindowSpec extends AnyFunSpec {
  implicit def logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  it("Write and read two entries") {
    val test: IO[Vector[(Long, Long)]] = acquireWindow(2.minutes).use { window =>
      val currTs = IO.pure(System.currentTimeMillis())
      for {
        tsOne          <- currTs
        _              <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne <- IO.pure(Transaction("Nike", 240, 1581256213, tsOne))
        _              <- window._1.putWindow(transactionOne)
        _              <- logger.info(s"First Transaction Success")
        tsTwo          <- currTs
        _              <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo <- IO.pure(Transaction("Addidas", 220, 1581256214, tsTwo))
        _              <- window._1.putWindow(transactionTwo)
        _              <- logger.info(s"Second Transaction Success")
        entry1         <- window._1.getWindow("Nike", 240)
        entryOne       <- IO.pure(entry1.get)
        entry2         <- window._1.getWindow("Addidas", 220)
        entryTwo       <- IO.pure(entry2.get)
      } yield Vector(entryOne, entryTwo).flatten
    }
    val testOutputVector: Vector[(Long, Long)] = test.unsafeToFuture().value.get.toOption.get

    assert(testOutputVector(0)._1 == 1581256213)
    assert(testOutputVector(1)._1 == 1581256214)
  }

  it("Write and read two entries with same key") {
    val test: IO[mutable.Seq[(Long, Long)]] = acquireWindow(20.seconds).use { window =>
      val currTs = IO.pure(System.currentTimeMillis())
      for {
        tsOne          <- currTs
        _              <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne <- IO.pure(Transaction("Nike", 240, 1581256223, tsOne))
        _              <- window._1.putWindow(transactionOne)
        _              <- logger.info(s"First Transaction Success")
        tsTwo          <- currTs
        _              <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo <- IO.pure(Transaction("Nike", 240, 1581256224, tsTwo))
        _              <- window._1.putWindow(transactionTwo)
        _              <- logger.info(s"Second Transaction Success")
        entry1         <- window._1.getWindow("Nike", 240)
        entryOne       <- IO.pure(entry1.get)
      } yield entryOne
    }
    val testOutputSeq: mutable.Seq[(Long, Long)] = test.unsafeToFuture().value.get.toOption.get
    assert(testOutputSeq.size == 2)
    assert(testOutputSeq.head._1 == 1581256223)
    assert(testOutputSeq(1)._1 == 1581256224)
  }

  it("Older entry of the same key should expire") {
    val test: IO[mutable.Seq[(Long, Long)]] = acquireWindow(20.seconds).use { window =>
      val currTs = IO.delay(System.currentTimeMillis())
      for {
        tsOne          <- currTs
        _              <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne <- IO.pure(Transaction("Nike", 240, 1581256233, tsOne))
        _              <- window._1.putWindow(transactionOne)
        _              <- logger.info(s"First Transaction Success")
        _              <- IO.delay(Thread.sleep(10000))
        tsTwo          <- currTs
        _              <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo <- IO.pure(Transaction("Nike", 240, 1581256234, tsTwo))
        _              <- window._1.putWindow(transactionTwo)
        _              <- logger.info(s"Second Transaction Success")
        _              <- IO.delay(Thread.sleep(14000))
        entry1         <- window._1.getWindow("Nike", 240)
        entryOne       <- IO.pure(entry1.get)
      } yield entryOne
    }
    val testOutputSeq: mutable.Seq[(Long, Long)] = test.unsafeToFuture().value.get.toOption.get
    assert(testOutputSeq.size == 1)
    assert(testOutputSeq.head._1 == 1581256234)
  }

  it("Size of window with distinct key transactions") {
    val test: IO[Int] = acquireWindow(20.seconds).use { window =>
      val currTs = IO.delay(System.currentTimeMillis())
      for {
        tsOne            <- currTs
        _                <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne   <- IO.pure(Transaction("Nike", 240, 1581256263, tsOne))
        _                <- window._1.putWindow(transactionOne)
        _                <- logger.info(s"First Transaction Success")
        tsTwo            <- currTs
        _                <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo   <- IO.pure(Transaction("Addidas", 240, 1581256264, tsTwo))
        _                <- window._1.putWindow(transactionTwo)
        tsThree          <- currTs
        _                <- logger.info(s"Using ${tsThree} for third transaction ")
        transactionThree <- IO.pure(Transaction("Puma", 240, 1581256265, tsThree))
        _                <- window._1.putWindow(transactionThree)
        _                <- logger.info(s"Third Transaction Success")
        size             <- window._1.getWindowSize
      } yield size
    }
    val testSize: Int = test.unsafeToFuture().value.get.toOption.get
    assert(testSize == 3)
  }

  it("Size of window with cache expiration and distinct key transactions") {
    val test = acquireWindow(20.seconds).use { window =>
      val currTs = IO.delay(System.currentTimeMillis())
      for {
        tsOne            <- currTs
        _                <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne   <- IO.pure(Transaction("Nike", 240, 1581256263, tsOne))
        _                <- window._1.putWindow(transactionOne)
        _                <- logger.info(s"First Transaction Success")
        tsTwo            <- currTs
        _                <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo   <- IO.pure(Transaction("Addidas", 240, 1581256264, tsTwo))
        _                <- window._1.putWindow(transactionTwo)
        _                <- IO.delay(Thread.sleep(60000))
        tsThree          <- currTs
        _                <- logger.info(s"Using ${tsThree} for third transaction ")
        transactionThree <- IO.pure(Transaction("Puma", 240, 1581256265, tsThree))
        _                <- window._1.putWindow(transactionThree)
        _                <- logger.info(s"Third Transaction Success")
        size             <- window._1.getWindowSize
      } yield size
    }
    val testSize: Int = test.unsafeToFuture().value.get.toOption.get
    assert(testSize == 1)
  }

  it("Size of window with entry expiration and multi-key transactions") {
    val test = acquireWindow(20.seconds).use { window =>
      val currTs = IO.delay(System.currentTimeMillis())
      for {
        tsOne            <- currTs
        _                <- logger.info(s"Using ${tsOne} for first transaction")
        transactionOne   <- IO.pure(Transaction("Nike", 240, 1581256263, tsOne))
        _                <- window._1.putWindow(transactionOne)
        _                <- logger.info(s"First Transaction Success")
        _                <- IO.delay(Thread.sleep(30000))
        tsTwo            <- currTs
        _                <- logger.info(s"Using ${tsTwo} for second transaction ")
        transactionTwo   <- IO.pure(Transaction("Nike", 240, 1581256264, tsTwo))
        _                <- window._1.putWindow(transactionTwo)
        tsThree          <- currTs
        _                <- logger.info(s"Using ${tsThree} for third transaction ")
        transactionThree <- IO.pure(Transaction("Nike", 240, 1581256265, tsThree))
        _                <- window._1.putWindow(transactionThree)
        _                <- logger.info(s"Third Transaction Success")
        size             <- window._1.getWindowSize
      } yield size
    }
    val testSize: Int = test.unsafeToFuture().value.get.toOption.get
    assert(testSize == 2)
  }

}
