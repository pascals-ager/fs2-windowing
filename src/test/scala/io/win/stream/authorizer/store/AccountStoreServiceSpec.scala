package io.win.stream.authorizer.store

import cats.effect.IO
import io.win.stream.authorizer.external.ExternalDomain
import io.win.stream.authorizer.external.ExternalDomain.Account
import io.win.stream.authorizer.stores.AccountStoreService
import org.scalatest.funspec.AnyFunSpec
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class AccountStoreServiceSpec extends AnyFunSpec {
  implicit def logger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  it("Account Init with None") {
    val test: IO[Option[ExternalDomain.Account]] = AccountStoreService.create().use { store =>
      for {
        init <- store.getAccount()
      } yield init
    }
    val testInitAccount: Option[ExternalDomain.Account] = test.unsafeToFuture().value.get.toOption.get
    assert(testInitAccount.isEmpty)
  }

  it("Account Write and Read one entry") {
    val firstAccount = Account(true, 100)
    val test: IO[(Option[Account], Option[Account])] = AccountStoreService.create().use { store =>
      for {
        firstPut <- store.putAccount(firstAccount)
        firstGet <- store.getAccount()
      } yield (firstPut, firstGet)
    }
    val testOneWriteRead: (Option[Account], Option[Account]) = test.unsafeToFuture().value.get.toOption.get
    assert(testOneWriteRead._1 == testOneWriteRead._2)
    assert(testOneWriteRead._1.contains(firstAccount))
  }
}
