package io.nubank.challenge.authorizer.validations

import cats.data.ValidatedNec
import cats.effect.IO
import io.nubank.challenge.authorizer.events.AccountsProcessor
import io.nubank.challenge.authorizer.external.ExternalDomain
import io.nubank.challenge.authorizer.external.ExternalDomain.{Account, Transaction}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.window.TransactionWindow.acquireWindow
import org.scalatest.funspec.AnyFunSpec
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

class ValidationServiceSpec extends AnyFunSpec {

  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  it("Should emit account-already-initialized") {
    val firstAccountEvent  = Account(true, 100)
    val secondAccountEvent = Account(true, 40)
    val test: IO[ValidatedNec[DomainValidation, Account]] = AccountStoreService.create().use { store =>
      for {
        acc        <- store.putAccount(firstAccountEvent)
        validation <- ValidationService.validateAccount(secondAccountEvent)(store)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isInvalid)
    assert(validation.fold(l => l.toChain.toList, r => List[DomainValidation]()) == List(`account-already-initialized`))
  }

  it("Should emit account-not-initialized with negative balance") {
    val firstAccountEvent = Account(true, -100)
    val test: IO[ValidatedNec[DomainValidation, Account]] = AccountStoreService.create().use { store =>
      for {
        validation <- ValidationService.validateAccount(firstAccountEvent)(store)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isInvalid)
    assert(validation.fold(l => l.toChain.toList, r => List[DomainValidation]()) == List(`account-not-initialized`))
  }

  it("Should initialize account as inactive") {
    val firstAccountEvent = Account(false, 100)
    val test: IO[ValidatedNec[DomainValidation, Account]] = AccountStoreService.create().use { store =>
      for {
        validation <- ValidationService.validateAccount(firstAccountEvent)(store)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isValid)
  }

  it("Should allow inactive accounts that can be activated") {
    val firstAccountEvent  = Account(false, 100)
    val secondAccountEvent = Account(true, 100)
    val test: IO[ValidatedNec[DomainValidation, Account]] = AccountStoreService.create().use { store =>
      for {
        acc        <- store.putAccount(firstAccountEvent)
        validation <- ValidationService.validateAccount(secondAccountEvent)(store)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isValid)
  }

  it("Should allow active accounts that can be inactivated") {
    val firstAccountEvent  = Account(true, 100)
    val secondAccountEvent = Account(false, 100)
    val test: IO[ValidatedNec[DomainValidation, Account]] = AccountStoreService.create().use { store =>
      for {
        acc        <- store.putAccount(firstAccountEvent)
        validation <- ValidationService.validateAccount(secondAccountEvent)(store)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isValid)
  }

  it("Should not allow balance changes during account activation") {
    val firstAccountEvent  = Account(true, 100)
    val secondAccountEvent = Account(false, 50)
    val test: IO[ValidatedNec[DomainValidation, Account]] = AccountStoreService.create().use { store =>
      for {
        acc        <- store.putAccount(firstAccountEvent)
        validation <- ValidationService.validateAccount(secondAccountEvent)(store)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isInvalid)
    assert(validation.fold(l => l.toChain.toList, r => List[DomainValidation]()) == List(`account-already-initialized`))
  }

  it("Should emit account-not-initialized on Transaction") {
    val firstTransaction = Transaction("Nike", 250, 1581256223, System.currentTimeMillis())
    val test: IO[ValidatedNec[DomainValidation, Transaction]] = AccountStoreService.create().use { store =>
      for {
        acc        <- store.getAccount()
        validation <- ValidationService.validatedAccountActive(firstTransaction, acc)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isInvalid)
    assert(validation.fold(l => l.toChain.toList, r => List[DomainValidation]()) == List(`account-not-initialized`))
  }

  it("Should emit card-not-active on Transaction") {
    val firstAccountEvent = Account(false, 100)
    val firstTransaction  = Transaction("Nike", 250, 1581256223, System.currentTimeMillis())
    val test: IO[ValidatedNec[DomainValidation, Transaction]] = AccountStoreService.create().use { store =>
      for {
        acc        <- store.putAccount(firstAccountEvent)
        validation <- ValidationService.validatedAccountActive(firstTransaction, acc)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isInvalid)
    assert(validation.fold(l => l.toChain.toList, r => List[DomainValidation]()) == List(`card-not-active`))
  }

  it("Should emit insufficient-limit on Transaction") {
    val firstAccountEvent = Account(true, 100)
    val firstTransaction  = Transaction("Nike", 250, 1581256223, System.currentTimeMillis())
    val test: IO[ValidatedNec[DomainValidation, Transaction]] = AccountStoreService.create().use { store =>
      for {
        acc        <- store.putAccount(firstAccountEvent)
        validation <- ValidationService.validatedAccountBalance(firstTransaction, acc)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isInvalid)
    assert(validation.fold(l => l.toChain.toList, r => List[DomainValidation]()) == List(`insufficient-limit`))
  }

  it("Should emit high-frequency-small-interval on Transaction") {
    val firstTransaction  = Transaction("Apple", 250, 1581256223, System.currentTimeMillis())
    val secondTransaction = Transaction("Nike", 250, 1581256224, System.currentTimeMillis())
    val thirdTransaction  = Transaction("Adidas", 250, 1581256225, System.currentTimeMillis())
    val fourthTransaction = Transaction("Puma", 250, 1581256226, System.currentTimeMillis())
    val test: IO[ValidatedNec[DomainValidation, Transaction]] = acquireWindow(30.seconds).use { window =>
      for {
        transactionOne   <- window._1.putTransaction(firstTransaction)
        transactiontwo   <- window._1.putTransaction(secondTransaction)
        transactionthree <- window._1.putTransaction(thirdTransaction)
        validation       <- ValidationService.validatedTransactionFrequency(fourthTransaction)(window._1)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isInvalid)
    assert(
      validation.fold(l => l.toChain.toList, r => List[DomainValidation]()) == List(`high-frequency-small-interval`)
    )
  }

  it("Should emit high-frequency-small-interval on Transaction with duplicate keys") {
    val firstTransaction  = Transaction("Apple", 250, 1581256223, System.currentTimeMillis())
    val secondTransaction = Transaction("Apple", 250, 1581256224, System.currentTimeMillis())
    val thirdTransaction  = Transaction("Apple", 250, 1581256225, System.currentTimeMillis())
    val fourthTransaction = Transaction("Apple", 250, 1581256226, System.currentTimeMillis())
    val test: IO[ValidatedNec[DomainValidation, Transaction]] = acquireWindow(30.seconds).use { window =>
      for {
        transactionOne   <- window._1.putTransaction(firstTransaction)
        transactiontwo   <- window._1.putTransaction(secondTransaction)
        transactionthree <- window._1.putTransaction(thirdTransaction)
        validation       <- ValidationService.validatedTransactionFrequency(fourthTransaction)(window._1)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isInvalid)
    assert(
      validation.fold(l => l.toChain.toList, r => List[DomainValidation]()) == List(`high-frequency-small-interval`)
    )
  }

  it("Should emit doubled-transaction on Transaction with combination of duplicate and distinct keys") {
    val firstTransaction  = Transaction("Apple", 250, 1581256223, System.currentTimeMillis())
    val secondTransaction = Transaction("Apple", 250, 1581256224, System.currentTimeMillis())
    val test: IO[ValidatedNec[DomainValidation, Transaction]] = acquireWindow(30.seconds).use { window =>
      for {
        transactionOne <- window._1.putTransaction(firstTransaction)
        validation     <- ValidationService.validatedDoubledTransaction(secondTransaction)(window._1)
      } yield validation
    }
    val validation = test.unsafeRunSync()
    assert(validation.isInvalid)
    assert(
      validation.fold(l => l.toChain.toList, r => List[DomainValidation]()) == List(`doubled-transaction`)
    )
  }
}
