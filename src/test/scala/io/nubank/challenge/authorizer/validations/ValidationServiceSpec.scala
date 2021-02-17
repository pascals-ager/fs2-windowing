package io.nubank.challenge.authorizer.validations

import cats.effect.IO
import io.nubank.challenge.authorizer.external.ExternalDomain
import io.nubank.challenge.authorizer.external.ExternalDomain.{Account, Transaction}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.validations.ValidationService._
import io.nubank.challenge.authorizer.window.ConcurrentWindow.acquireWindow
import org.scalatest.funspec.AnyFunSpec
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class ValidationServiceSpec extends AnyFunSpec {

  implicit def logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  it("It should validate AccountEvent") {
    val firstAccount = Account(true, 100)
    val test: IO[(ExternalDomain.AccountState, Option[Account])] = AccountStoreService.create().use { store =>
      for {
        accState <- validateAndPut(firstAccount)(store)
        firstGet <- store.getAccount()
      } yield (accState, firstGet)
    }

    val testOneWriteRead: (ExternalDomain.AccountState, Option[Account]) = test.unsafeToFuture().value.get.toOption.get
    assert(testOneWriteRead._1.account == testOneWriteRead._2)
    assert(testOneWriteRead._1.account.contains(firstAccount))
    assert(testOneWriteRead._1.violations.isEmpty)
  }

  it("It should invalidate second active AccountEvent") {
    val firstAccount  = Account(true, 100)
    val secondAccount = Account(true, 40)
    val test: IO[(ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account])] =
      AccountStoreService.create().use { store =>
        for {
          accStateOne <- validateAndPut(firstAccount)(store)
          accStateTwo <- validateAndPut(secondAccount)(store)
          firstGet    <- store.getAccount()
        } yield (accStateOne, accStateTwo, firstGet)
      }

    val testOneWriteRead: (ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account]) =
      test.unsafeToFuture().value.get.toOption.get
    assert(testOneWriteRead._1.account.contains(firstAccount))
    assert(testOneWriteRead._2.account.isEmpty)
    assert(testOneWriteRead._1.violations.isEmpty)
    assert(testOneWriteRead._2.violations.length == 1)
    assert(testOneWriteRead._2.violations.head == AccountAlreadyInit)
    assert(testOneWriteRead._3.contains(firstAccount))
  }

  it("Should apply validation rules on incorrect AccountEvent") {

    val firstAccount  = Account(true, -40) /*Init balance negative*/
    val secondAccount = Account(false, 100) /*active false account initiation is allowed*/
    val test: IO[(ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account])] =
      AccountStoreService.create().use { store =>
        for {
          accStateOne <- validateAndPut(firstAccount)(store)
          accStateTwo <- validateAndPut(secondAccount)(store)
          firstGet    <- store.getAccount()
        } yield (accStateOne, accStateTwo, firstGet)
      }

    val testOneWriteRead: (ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account]) =
      test.unsafeToFuture().value.get.toOption.get

    assert(testOneWriteRead._1.account.isEmpty)
    assert(testOneWriteRead._2.account.contains(secondAccount))
    assert(testOneWriteRead._1.violations.length == 1)
    assert(testOneWriteRead._2.violations.isEmpty)
    assert(testOneWriteRead._1.violations.head == AccountNotInit)
    assert(testOneWriteRead._3.contains(secondAccount))
  }

  it("Should validate single transaction") {
    val beforeTransactionAccount = Account(true, 400)
    val afterTransactionAccount  = Account(true, 160)

    val test: IO[
      (ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account], Option[mutable.Seq[(Long, Long)]])
    ] = AccountStoreService.create().use { store =>
      acquireWindow(2.minutes).use { window =>
        val currTs = IO.pure(System.currentTimeMillis())

        for {
          tsOne          <- currTs
          _              <- logger.info(s"Using ${tsOne} for first transaction")
          transactionOne <- IO.pure(Transaction("Nike", 240, 1581256223, tsOne))
          acctStateOne   <- validateAndPut(beforeTransactionAccount)(store)
          acctStateTwo   <- validateAndPut(transactionOne)(store, window._1)
          acct           <- store.getAccount()
          entry          <- window._1.getWindow("Nike", 240)
        } yield (acctStateOne, acctStateTwo, acct, entry)

      }
    }
    val testOutputSeq: (
        ExternalDomain.AccountState,
        ExternalDomain.AccountState,
        Option[Account],
        Option[mutable.Seq[(Long, Long)]]
    ) = test.unsafeToFuture().value.get.toOption.get
    assert(testOutputSeq._1.account.contains(beforeTransactionAccount))
    assert(testOutputSeq._1.violations.isEmpty)
    assert(testOutputSeq._2.account.contains(afterTransactionAccount))
    assert(testOutputSeq._2.violations.isEmpty)
    assert(testOutputSeq._3.contains(afterTransactionAccount))
    assert(testOutputSeq._4.get.size == 1)
    assert(testOutputSeq._4.get.head._1 == 1581256223)
  }

  it("Should invalidate with single transaction - card-inactive") {
    val beforeTransactionAccount = Account(false, 240)

    val test: IO[
      (ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account], Option[mutable.Seq[(Long, Long)]])
    ] = AccountStoreService.create().use { store =>
      acquireWindow(2.minutes).use { window =>
        val currTs = IO.pure(System.currentTimeMillis())

        for {
          tsOne          <- currTs
          _              <- logger.info(s"Using ${tsOne} for first transaction")
          transactionOne <- IO.pure(Transaction("Nike", 240, 1581256223, tsOne))
          acctStateOne   <- validateAndPut(beforeTransactionAccount)(store)
          acctStateTwo   <- validateAndPut(transactionOne)(store, window._1)
          acct           <- store.getAccount()
          entry          <- window._1.getWindow("Nike", 240)
        } yield (acctStateOne, acctStateTwo, acct, entry)

      }
    }
    val testOutputSeq: (
        ExternalDomain.AccountState,
        ExternalDomain.AccountState,
        Option[Account],
        Option[mutable.Seq[(Long, Long)]]
    ) = test.unsafeToFuture().value.get.toOption.get
    assert(testOutputSeq._1.account.contains(beforeTransactionAccount))
    assert(testOutputSeq._1.violations.isEmpty)
    assert(testOutputSeq._2.account.contains(beforeTransactionAccount))
    assert(testOutputSeq._2.violations.length == 1)
    assert(testOutputSeq._2.violations.head == CardInactive)
    assert(testOutputSeq._3.contains(beforeTransactionAccount))
    assert(testOutputSeq._4.isEmpty)
  }

  it("Should invalidate with single transaction - insufficient-limit") {
    val beforeTransactionAccount = Account(true, 230)

    val test: IO[
      (ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account], Option[mutable.Seq[(Long, Long)]])
    ] = AccountStoreService.create().use { store =>
      acquireWindow(2.minutes).use { window =>
        val currTs = IO.pure(System.currentTimeMillis())

        for {
          tsOne          <- currTs
          _              <- logger.info(s"Using ${tsOne} for first transaction")
          transactionOne <- IO.pure(Transaction("Nike", 240, 1581256223, tsOne))
          acctStateOne   <- validateAndPut(beforeTransactionAccount)(store)
          acctStateTwo   <- validateAndPut(transactionOne)(store, window._1)
          acct           <- store.getAccount()
          entry          <- window._1.getWindow("Nike", 240)
        } yield (acctStateOne, acctStateTwo, acct, entry)

      }
    }
    val testOutputSeq: (
        ExternalDomain.AccountState,
        ExternalDomain.AccountState,
        Option[Account],
        Option[mutable.Seq[(Long, Long)]]
    ) = test.unsafeToFuture().value.get.toOption.get
    assert(testOutputSeq._1.account.contains(beforeTransactionAccount))
    assert(testOutputSeq._1.violations.isEmpty)
    assert(testOutputSeq._2.account.contains(beforeTransactionAccount))
    assert(testOutputSeq._2.violations.length == 1)
    assert(testOutputSeq._2.violations.head == InsufficientLimit)
    assert(testOutputSeq._3.contains(beforeTransactionAccount))
    assert(testOutputSeq._4.isEmpty)
  }

  it("Should invalidate with multiple transactions - insufficient-limit and high-frequency-small-interval") {
    val beforeTransactionAccount      = Account(true, 1000)
    val afterFirstTransactionAccount  = Account(true, 750)
    val afterSecondTransactionAccount = Account(true, 500)
    val afterThirdTransactionAccount  = Account(true, 0)

    val test: IO[
      (
          ExternalDomain.AccountState,
          ExternalDomain.AccountState,
          ExternalDomain.AccountState,
          ExternalDomain.AccountState,
          ExternalDomain.AccountState,
          Option[mutable.Seq[(Long, Long)]],
          Option[mutable.Seq[(Long, Long)]],
          Option[mutable.Seq[(Long, Long)]],
          Option[mutable.Seq[(Long, Long)]]
      )
    ] = AccountStoreService.create().use { store =>
      acquireWindow(2.minutes).use { window =>
        val currTs = IO.pure(System.currentTimeMillis())

        for {
          tsOne            <- currTs
          _                <- logger.info(s"Using ${tsOne} for first transaction")
          transactionOne   <- IO.pure(Transaction("Nike", 250, 1581256233, tsOne))
          acctStateOne     <- validateAndPut(beforeTransactionAccount)(store)
          acctStateTwo     <- validateAndPut(transactionOne)(store, window._1)
          tsTwo            <- currTs
          _                <- logger.info(s"Using ${tsTwo} for second transaction")
          transactionTwo   <- IO.pure(Transaction("Adidas", 250, 1581256234, tsTwo))
          acctStateThree   <- validateAndPut(transactionTwo)(store, window._1)
          tsThree          <- currTs
          _                <- logger.info(s"Using ${tsThree} for third transaction")
          transactionThree <- IO.pure(Transaction("Apple", 500, 1581256235, tsThree))
          acctStateFour    <- validateAndPut(transactionThree)(store, window._1)
          tsFour           <- currTs
          _                <- logger.info(s"Using ${tsFour} for fourth transaction")
          transactionFour  <- IO.pure(Transaction("Apple", 10, 1581256236, tsFour))
          acctStateFive    <- validateAndPut(transactionFour)(store, window._1)
          entryOne         <- window._1.getWindow("Nike", 250)
          entryTwo         <- window._1.getWindow("Adidas", 250)
          entryThree       <- window._1.getWindow("Apple", 500)
          entryFour        <- window._1.getWindow("Apple", 10)
        } yield (
          acctStateOne,
          acctStateTwo,
          acctStateThree,
          acctStateFour,
          acctStateFive,
          entryOne,
          entryTwo,
          entryThree,
          entryFour
        )

      }
    }
    val testOutputSeq: (
        ExternalDomain.AccountState,
        ExternalDomain.AccountState,
        ExternalDomain.AccountState,
        ExternalDomain.AccountState,
        ExternalDomain.AccountState,
        Option[mutable.Seq[(Long, Long)]],
        Option[mutable.Seq[(Long, Long)]],
        Option[mutable.Seq[(Long, Long)]],
        Option[mutable.Seq[(Long, Long)]]
    ) = test.unsafeToFuture().value.get.toOption.get
    assert(testOutputSeq._1.account.contains(beforeTransactionAccount))
    assert(testOutputSeq._1.violations.isEmpty)
    assert(testOutputSeq._2.account.contains(afterFirstTransactionAccount))
    assert(testOutputSeq._2.violations.isEmpty)
    assert(testOutputSeq._3.account.contains(afterSecondTransactionAccount))
    assert(testOutputSeq._3.violations.isEmpty)
    assert(testOutputSeq._4.account.contains(afterThirdTransactionAccount))
    assert(testOutputSeq._4.violations.isEmpty)
    assert(testOutputSeq._5.account.contains(afterThirdTransactionAccount))
    assert(testOutputSeq._5.violations.size == 2)
    assert(testOutputSeq._5.violations.toSet == Set(HighFreqTransaction, InsufficientLimit))
    assert(testOutputSeq._6.get.size == 1)
    assert(testOutputSeq._6.get.head._1 == 1581256233)
    assert(testOutputSeq._7.get.size == 1)
    assert(testOutputSeq._7.get.head._1 == 1581256234)
    assert(testOutputSeq._8.get.size == 1)
    assert(testOutputSeq._8.get.head._1 == 1581256235)
    assert(testOutputSeq._9.isEmpty)

  }

  it("Should invalidate with multiple transactions - insufficient-limit and doubled-transaction") {
    val beforeTransactionAccount     = Account(true, 1001)
    val afterFirstTransactionAccount = Account(true, 1)

    val test: IO[
      (
          ExternalDomain.AccountState,
          ExternalDomain.AccountState,
          ExternalDomain.AccountState,
          Option[mutable.Seq[(Long, Long)]]
      )
    ] = AccountStoreService.create().use { store =>
      acquireWindow(2.minutes).use { window =>
        val currTs = IO.pure(System.currentTimeMillis())

        for {
          tsOne          <- currTs
          _              <- logger.info(s"Using ${tsOne} for first transaction")
          transactionOne <- IO.pure(Transaction("Apple", 1000, 1581256243, tsOne))
          acctStateOne   <- validateAndPut(beforeTransactionAccount)(store)
          acctStateTwo   <- validateAndPut(transactionOne)(store, window._1)
          tsTwo          <- currTs
          _              <- logger.info(s"Using ${tsTwo} for second transaction")
          transactionTwo <- IO.pure(Transaction("Apple", 1000, 1581256244, tsTwo))
          acctStateThree <- validateAndPut(transactionTwo)(store, window._1)

          entryOne <- window._1.getWindow("Apple", 1000)

        } yield (
          acctStateOne,
          acctStateTwo,
          acctStateThree,
          entryOne
        )

      }
    }
    val testOutputSeq: (
        ExternalDomain.AccountState,
        ExternalDomain.AccountState,
        ExternalDomain.AccountState,
        Option[mutable.Seq[(Long, Long)]]
    ) = test.unsafeToFuture().value.get.toOption.get
    assert(testOutputSeq._1.account.contains(beforeTransactionAccount))
    assert(testOutputSeq._1.violations.isEmpty)
    assert(testOutputSeq._2.account.contains(afterFirstTransactionAccount))
    assert(testOutputSeq._2.violations.isEmpty)
    assert(testOutputSeq._3.account.contains(afterFirstTransactionAccount))
    assert(testOutputSeq._3.violations.size == 2)
    assert(testOutputSeq._3.violations.toSet == Set(DoubledTransaction, InsufficientLimit))
    assert(testOutputSeq._4.get.size == 1)
    assert(testOutputSeq._4.get.head._1 == 1581256243)

  }

}
