package io.win.stream.authorizer.events

import cats.effect.concurrent.Semaphore
import cats.effect.{ContextShift, IO, Timer}
import io.win.stream.authorizer.external.ExternalDomain
import io.win.stream.authorizer.external.ExternalDomain.{Account, Transaction}
import io.win.stream.authorizer.stores.AccountStoreService
import io.win.stream.authorizer.validations.{
  `card-not-active`,
  `doubled-transaction`,
  `high-frequency-small-interval`,
  `insufficient-limit`
}
import io.win.stream.authorizer.window.TransactionWindow.acquireWindow
import org.scalatest.funspec.AnyFunSpec
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.collection.mutable
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration.DurationInt

class TransactionsProcessorSpec extends AnyFunSpec {

  private implicit val cs: ContextShift[IO]          = IO.contextShift(global)
  private implicit val timer: Timer[IO]              = IO.timer(global)
  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  implicit val semaphore                             = Semaphore[IO](1)

  it("Should validate single transaction") {
    val beforeTransactionAccount = Account(true, 400)
    val afterTransactionAccount  = Account(true, 160)

    val test: IO[
      (ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account], Option[mutable.Seq[(Long, Long)]])
    ] = AccountStoreService.create().use { store =>
      acquireWindow(2.minutes).use { window =>
        val currTs = IO.pure(System.currentTimeMillis())
        for {
          sem <- semaphore
          accountsHandler     = new AccountsProcessor(store)
          transactionsHandler = new TransactionsProcessor(store, window._1)
          tsOne          <- currTs
          _              <- logger.info(s"Using ${tsOne} for first transaction")
          transactionOne <- IO.pure(Transaction("Nike", 240, 1581256223, tsOne))
          acctStateOne   <- accountsHandler.validateAndPutAccount(beforeTransactionAccount)
          acctStateTwo   <- transactionsHandler.validateAndPutTransaction(transactionOne)
          acct           <- store.getAccount()
          entry          <- window._1.getTransactionEntry("Nike", 240)
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
          sem <- semaphore
          accountsHandler     = new AccountsProcessor(store)
          transactionsHandler = new TransactionsProcessor(store, window._1)
          tsOne          <- currTs
          _              <- logger.info(s"Using ${tsOne} for first transaction")
          transactionOne <- IO.pure(Transaction("Nike", 240, 1581256223, tsOne))
          acctStateOne   <- accountsHandler.validateAndPutAccount(beforeTransactionAccount)
          acctStateTwo   <- transactionsHandler.validateAndPutTransaction(transactionOne)
          acct           <- store.getAccount()
          entry          <- window._1.getTransactionEntry("Nike", 240)
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
    assert(testOutputSeq._2.violations.head == `card-not-active`)
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
          sem <- semaphore
          accountsHandler     = new AccountsProcessor(store)
          transactionsHandler = new TransactionsProcessor(store, window._1)
          tsOne          <- currTs
          _              <- logger.info(s"Using ${tsOne} for first transaction")
          transactionOne <- IO.pure(Transaction("Nike", 240, 1581256223, tsOne))
          acctStateOne   <- accountsHandler.validateAndPutAccount(beforeTransactionAccount)
          acctStateTwo   <- transactionsHandler.validateAndPutTransaction(transactionOne)
          acct           <- store.getAccount()
          entry          <- window._1.getTransactionEntry("Nike", 240)
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
    assert(testOutputSeq._2.violations.head == `insufficient-limit`)
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
          sem <- semaphore
          accountsHandler     = new AccountsProcessor(store)
          transactionsHandler = new TransactionsProcessor(store, window._1)
          tsOne            <- currTs
          _                <- logger.info(s"Using ${tsOne} for first transaction")
          transactionOne   <- IO.pure(Transaction("Nike", 250, 1581256233, tsOne))
          acctStateOne     <- accountsHandler.validateAndPutAccount(beforeTransactionAccount)
          acctStateTwo     <- transactionsHandler.validateAndPutTransaction(transactionOne)
          tsTwo            <- currTs
          _                <- logger.info(s"Using ${tsTwo} for second transaction")
          transactionTwo   <- IO.pure(Transaction("Adidas", 250, 1581256234, tsTwo))
          acctStateThree   <- transactionsHandler.validateAndPutTransaction(transactionTwo)
          tsThree          <- currTs
          _                <- logger.info(s"Using ${tsThree} for third transaction")
          transactionThree <- IO.pure(Transaction("Apple", 500, 1581256235, tsThree))
          acctStateFour    <- transactionsHandler.validateAndPutTransaction(transactionThree)
          tsFour           <- currTs
          _                <- logger.info(s"Using ${tsFour} for fourth transaction")
          transactionFour  <- IO.pure(Transaction("Apple", 10, 1581256236, tsFour))
          acctStateFive    <- transactionsHandler.validateAndPutTransaction(transactionFour)
          entryOne         <- window._1.getTransactionEntry("Nike", 250)
          entryTwo         <- window._1.getTransactionEntry("Adidas", 250)
          entryThree       <- window._1.getTransactionEntry("Apple", 500)
          entryFour        <- window._1.getTransactionEntry("Apple", 10)
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
    assert(testOutputSeq._5.violations.toSet == Set(`high-frequency-small-interval`, `insufficient-limit`))
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
          sem <- semaphore
          accountsHandler     = new AccountsProcessor(store)
          transactionsHandler = new TransactionsProcessor(store, window._1)
          tsOne          <- currTs
          _              <- logger.info(s"Using ${tsOne} for first transaction")
          transactionOne <- IO.pure(Transaction("Apple", 1000, 1581256243, tsOne))
          acctStateOne   <- accountsHandler.validateAndPutAccount(beforeTransactionAccount)
          acctStateTwo   <- transactionsHandler.validateAndPutTransaction(transactionOne)
          tsTwo          <- currTs
          _              <- logger.info(s"Using ${tsTwo} for second transaction")
          transactionTwo <- IO.pure(Transaction("Apple", 1000, 1581256244, tsTwo))
          acctStateThree <- transactionsHandler.validateAndPutTransaction(transactionTwo)

          entryOne <- window._1.getTransactionEntry("Apple", 1000)

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
    assert(testOutputSeq._3.violations.toSet == Set(`doubled-transaction`, `insufficient-limit`))
    assert(testOutputSeq._4.get.size == 1)
    assert(testOutputSeq._4.get.head._1 == 1581256243)

  }
}
