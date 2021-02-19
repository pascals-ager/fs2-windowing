package io.nubank.challenge.authorizer.validations

import cats.data.ValidatedNec
import cats.effect.IO
import cats.effect.concurrent.Semaphore
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
          else IO.pure(`account-not-initialized`.invalidNec)
        case Some(acc) =>
          if ((acc.`active-card` && !(newAccount.`active-card`) && (acc.`available-limit` == newAccount.`available-limit`)) ||
              (!(acc.`active-card`) && newAccount.`active-card` && (acc.`available-limit` == newAccount.`available-limit`)))
            IO.pure(newAccount.validNec)
          else IO.pure(`account-already-initialized`.invalidNec)
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
            IO.pure(`card-not-active`.invalidNec)
          }
        case None => IO.pure(`account-not-initialized`.invalidNec)
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
            IO.pure(`insufficient-limit`.invalidNec)
          }
        case None => IO.pure(`account-not-initialized`.invalidNec)
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
        IO.pure(`high-frequency-small-interval`.invalidNec)
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
            IO.pure(`doubled-transaction`.invalidNec)
          }
        case None => IO.pure(transaction.validNec)
      }
    } yield valid

}
