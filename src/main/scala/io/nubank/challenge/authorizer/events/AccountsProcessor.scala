package io.win.stream.authorizer.events

import cats.effect.IO
import cats.effect.concurrent.Semaphore
import io.win.stream.authorizer.external.ExternalDomain.{Account, AccountState}
import io.win.stream.authorizer.stores.AccountStoreService
import io.win.stream.authorizer.validations.ValidationService.validateAccount
import org.typelevel.log4cats.Logger

class AccountsProcessor(store: AccountStoreService)(implicit logger: Logger[IO]) {

  /**
    * Validate an AccountEvent and apply it to AccountStore
    * @param  account: The account to be validated and applied to AccountStore
    * @return Returns state of the Account and any violations that may have occurred
    */
  def validateAndPutAccount(
      account: Account
  ): IO[AccountState] = {
    for {
      valid <- validateAccount(account)(store)
      accState <- valid.toEither match {
        case Right(value) => store.putAccount(value).map(old => AccountState(Some(value), List()))
        case Left(ex)     => IO.pure(ex.toChain.toList).map(errs => AccountState(None, errs))
      }
      _ <- logger.debug(s"Account state modified to: ${accState.account}")
    } yield accState
  }
}
