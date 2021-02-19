package io.nubank.challenge.authorizer.events

import cats.effect.concurrent.Semaphore
import cats.effect.{ContextShift, IO, Timer}
import fs2.Stream
import io.nubank.challenge.authorizer.external.ExternalDomain
import io.nubank.challenge.authorizer.external.ExternalDomain.Account
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.validations.{`account-already-initialized`, `account-not-initialized`}
import io.nubank.challenge.authorizer.window.TransactionWindow
import io.nubank.challenge.authorizer.window.TransactionWindow.acquireWindow
import monix.execution.Cancelable
import org.scalatest.funspec.AnyFunSpec
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration.DurationInt

class AccountsProcessorSpec extends AnyFunSpec {

  private implicit val cs: ContextShift[IO]          = IO.contextShift(global)
  private implicit val timer: Timer[IO]              = IO.timer(global)
  implicit val logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  implicit val semaphore: IO[Semaphore[IO]] = Semaphore[IO](1)

  it("It should validate AccountEvent") {
    val firstAccount = Account(true, 100)
    val test: IO[(ExternalDomain.AccountState, Option[Account])] = AccountStoreService.create().use { store =>
      for {
        sem <- semaphore
        accountsHandler = new AccountsProcessor(store)
        accState <- accountsHandler.validateAndPutAccount(firstAccount)(sem)
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
          sem <- semaphore
          accountsHandler = new AccountsProcessor(store)
          accStateOne <- accountsHandler.validateAndPutAccount(firstAccount)(sem)
          accStateTwo <- accountsHandler.validateAndPutAccount(secondAccount)(sem)
          firstGet    <- store.getAccount()
        } yield (accStateOne, accStateTwo, firstGet)
      }

    val testOneWriteRead: (ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account]) =
      test.unsafeToFuture().value.get.toOption.get
    assert(testOneWriteRead._1.account.contains(firstAccount))
    assert(testOneWriteRead._2.account.isEmpty)
    assert(testOneWriteRead._1.violations.isEmpty)
    assert(testOneWriteRead._2.violations.length == 1)
    assert(testOneWriteRead._2.violations.head == `account-already-initialized`)
    assert(testOneWriteRead._3.contains(firstAccount))
  }

  it("Should apply validation rules on incorrect AccountEvent") {

    val firstAccount  = Account(true, -40) /*Init balance negative*/
    val secondAccount = Account(false, 100) /*active false account initiation is allowed*/
    val test: IO[(ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account])] =
      AccountStoreService.create().use { store =>
        for {
          sem <- semaphore
          accountsHandler = new AccountsProcessor(store)
          accStateOne <- accountsHandler.validateAndPutAccount(firstAccount)(sem)
          accStateTwo <- accountsHandler.validateAndPutAccount(secondAccount)(sem)
          firstGet    <- store.getAccount()
        } yield (accStateOne, accStateTwo, firstGet)
      }

    val testOneWriteRead: (ExternalDomain.AccountState, ExternalDomain.AccountState, Option[Account]) =
      test.unsafeToFuture().value.get.toOption.get

    assert(testOneWriteRead._1.account.isEmpty)
    assert(testOneWriteRead._2.account.contains(secondAccount))
    assert(testOneWriteRead._1.violations.length == 1)
    assert(testOneWriteRead._2.violations.isEmpty)
    assert(testOneWriteRead._1.violations.head == `account-not-initialized`)
    assert(testOneWriteRead._3.contains(secondAccount))
  }

}
