package io.nubank.challenge.authorizer.validations

import cats.data.ValidatedNec
import cats.effect.IO
import cats.implicits._
import io.nubank.challenge.authorizer.external.ExternalDomain.{Account, AccountState, Transaction}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.window.ConcurrentWindow

object ValidationService {

  def validateAccount(newAccount: Account, oldAccount: Option[Account]): IO[ValidatedNec[DomainValidation, Account]] =
    for {
      valid <- oldAccount match {
        case None =>
          if (newAccount.`available-limit` >= 0) IO.pure(newAccount.validNec)
          else IO.pure(AccountNotInit.invalidNec)
        case Some(acc) =>
          if ((acc.`active-card` && !(newAccount.`active-card`) && (acc.`available-limit` == newAccount.`available-limit`)) ||
              (!(acc.`active-card`) && newAccount.`active-card` && (acc.`available-limit` == newAccount.`available-limit`)))
            IO.pure(newAccount.validNec)
          else IO.pure(AccountAlreadyInit.invalidNec)
      }
    } yield valid

  def validatedAccountActive(
      transaction: Transaction,
      transactionAccount: Option[Account]
  ): IO[ValidatedNec[DomainValidation, Transaction]] =
    for {
      valid <- transactionAccount match {
        case Some(acc) =>
          if (acc.`active-card`) {
            IO.pure(transaction.validNec)
          } else {
            IO.pure(CardInactive.invalidNec)
          }
        case None => IO.pure(AccountNotInit.invalidNec)
      }
    } yield valid

  def validatedAccountBalance(
      transaction: Transaction,
      transactionAccount: Option[Account]
  ): IO[ValidatedNec[DomainValidation, Transaction]] =
    for {
      valid <- transactionAccount match {
        case Some(acc) =>
          if (acc.`available-limit` >= transaction.amount) {
            IO.pure(transaction.validNec)
          } else {
            IO.pure(InsufficientLimit.invalidNec)
          }
        case None => IO.pure(AccountNotInit.invalidNec)
      }
    } yield valid

  def validatedTransactionFrequency(
      transaction: Transaction
  )(implicit window: ConcurrentWindow): IO[ValidatedNec[DomainValidation, Transaction]] =
    for {
      transactionCount <- window.getWindowSize
      valid <- if (transactionCount < 3) {
        IO.pure(transaction.validNec)
      } else {
        IO.pure(HighFreqTransaction.invalidNec)
      }
    } yield valid

  def validatedDoubledTransaction(
      transaction: Transaction
  )(implicit window: ConcurrentWindow): IO[ValidatedNec[DomainValidation, Transaction]] =
    for {
      prevTransaction <- window.getWindow(transaction.merchant, transaction.amount)
      valid <- prevTransaction match {
        case Some(entry) =>
          /* This is pretty naive. In this scenario, this works.
           * Here the eviction interval is 2 mins and tolerance is 1 transaction per 2 mins
           * */
          if (entry.isEmpty) {
            IO.pure(transaction.validNec)
          } else {
            IO.pure(DoubledTransaction.invalidNec)
          }
        case None => IO.pure(transaction.validNec)
      }
    } yield valid

  def validateAndPut(account: Account)(implicit store: AccountStoreService): IO[AccountState] = {
    for {
      oldAcc <- store.getAccount()
      valid  <- validateAccount(account, oldAcc)
      accState <- valid.toEither match {
        case Right(value) => store.putAccount(value).map(old => AccountState(Some(value), List()))
        case Left(ex)     => IO.pure(ex.toChain.toList).map(errs => AccountState(None, errs))
      }
    } yield accState
  }

  def validateAndPut(
      transaction: Transaction
  )(implicit store: AccountStoreService, window: ConcurrentWindow): IO[AccountState] =
    for {
      transactionAccount       <- store.getAccount()
      acctActiveValidation     <- validatedAccountActive(transaction, transactionAccount)
      acctBalanceValidation    <- validatedAccountBalance(transaction, transactionAccount)
      transactFreqValidation   <- validatedTransactionFrequency(transaction)(window)
      transactDoubleValidation <- validatedDoubledTransaction(transaction)(window)
      validations <- IO.delay(
        Seq(acctActiveValidation, acctBalanceValidation, transactFreqValidation, transactDoubleValidation)
      )
      allValid  <- IO.delay(validations.forall(item => item.isValid))
      allErrors <- IO.delay(validations.flatMap(va => va.fold(l => l.toChain.toList, r => List[DomainValidation]())))
      acctState <- if (allValid) {
        store
          .putAccount(
            Account(transactionAccount.get.`active-card`, transactionAccount.get.`available-limit` - transaction.amount)
          )
          .flatMap { newAcc => window.putWindow(transaction).map(io => AccountState(newAcc, allErrors.toList)) }
      } else {
        IO.delay(AccountState(transactionAccount, allErrors.toList))
      }

    } yield acctState

}
