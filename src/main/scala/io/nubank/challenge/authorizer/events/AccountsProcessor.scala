package io.nubank.challenge.authorizer.events

import cats.effect.IO
import cats.effect.concurrent.Semaphore
import io.nubank.challenge.authorizer.external.ExternalDomain.{Account, AccountState}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.validations.ValidationService.validateAccount
import io.nubank.challenge.authorizer.window.TransactionWindow

class AccountsProcessor(store: AccountStoreService) {

  /**
    * Validate an AccountEvent and apply it to AccountStore
    * @param  account: The account to be validated and applied to AccountStore
    * @return Returns state of the Account and any violations that may have occurred
    */
  def validateAndPutAccount(
      account: Account
  )(implicit semaphore: Semaphore[IO]): IO[AccountState] = {
    for {
      acq   <- semaphore.acquire
      valid <- validateAccount(account)(store)
      accState <- valid.toEither match {
        case Right(value) => store.putAccount(value).map(old => AccountState(Some(value), List()))
        case Left(ex)     => IO.pure(ex.toChain.toList).map(errs => AccountState(None, errs))
      }
      rel <- semaphore.release
    } yield accState
  }
}
