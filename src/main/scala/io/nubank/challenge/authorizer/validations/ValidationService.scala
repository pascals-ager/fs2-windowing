package io.nubank.challenge.authorizer.validations

import cats.data.ValidatedNec
import cats.effect.IO
import cats.implicits._
import io.nubank.challenge.authorizer.external.ExternalDomain.{Account, AccountState, Transaction}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.window.TransactionWindow

object ValidationService {

  /**
   * @param newAccount: The account to be validated
   * @return Returns DomainValidation on invalid and Account on Valid
   */
  def validateAccount(
      newAccount: Account
  )(implicit store: AccountStoreService): IO[ValidatedNec[DomainValidation, Account]] =
    for {
      oldAccount <- store.getAccount()
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

  /**
   * Validate if the account used by the transaction is Active
   * @param transaction: The transaction to be validated
   * @param transactionAccount: The current state of the account against which the transaction is validated
   * @return Returns DomainValidation on invalid and Transaction on Valid
   */
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

  /**
   * Validate if the account used by the transaction has sufficient balance.
   * @param transaction: The transaction to be validated
   * @param transactionAccount: The current state of the account against which the transaction is validated
   * @return Returns DomainValidation on invalid and Transaction on Valid
   */
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

  /**
   * Validate if the transaction does not violate high frequency tolerance.
   * @param transaction: The transaction to be validated
   * @return Returns DomainValidation on invalid and Transaction on Valid
   */
  def validatedTransactionFrequency(
      transaction: Transaction
  )(implicit window: TransactionWindow): IO[ValidatedNec[DomainValidation, Transaction]] =
    for {
      transactionCount <- window.getWindowSize
      valid <- if (transactionCount < 3) {
        IO.pure(transaction.validNec)
      } else {
        IO.pure(HighFreqTransaction.invalidNec)
      }
    } yield valid

  /**
   * Validate if the transaction does not violate doubled transaction tolerance.
   * @param transaction: The transaction to be validated
   * @return Returns DomainValidation on invalid and Transaction on Valid
   */
  def validatedDoubledTransaction(
      transaction: Transaction
  )(implicit window: TransactionWindow): IO[ValidatedNec[DomainValidation, Transaction]] =
    for {
      prevTransaction <- window.getTransactionEntry(transaction.merchant, transaction.amount)
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

  /**
   * Validate an AccountEvent and apply it to AccountStore
   * @param  account: The account to be validated and applied to AccountStore
   * @return Returns state of the Account and any violations that may have occurred
   */
  def validateAndPut(account: Account)(implicit store: AccountStoreService): IO[AccountState] = {
    for {
      valid <- validateAccount(account)(store)
      accState <- valid.toEither match {
        case Right(value) => store.putAccount(value).map(old => AccountState(Some(value), List()))
        case Left(ex)     => IO.pure(ex.toChain.toList).map(errs => AccountState(None, errs))
      }
    } yield accState
  }
  /**
   * Validate an TransactionEvent and apply it the AccountStore as well TransactionWindow
   * @param transaction: The transaction to be validated and applied to AccountStore and TransactionWindow
   * @return Returns state of the Account used in the transactions with any violations that may have occurred
   */
  def validateAndPut(
      transaction: Transaction
  )(implicit store: AccountStoreService, window: TransactionWindow): IO[AccountState] =
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
          .flatMap { newAcc => window.putTransaction(transaction).map(io => AccountState(newAcc, allErrors.toList)) }
      } else {
        IO.delay(AccountState(transactionAccount, allErrors.toList))
      }

    } yield acctState

}
