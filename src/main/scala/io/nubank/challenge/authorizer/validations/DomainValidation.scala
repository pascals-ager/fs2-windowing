package io.nubank.challenge.authorizer.validations

import cats.data.ValidatedNec
import cats.effect.IO
import cats.implicits.catsSyntaxValidatedIdBinCompat0
import io.circe.generic.extras.semiauto.deriveEnumerationCodec
import io.circe.{Codec, Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.nubank.challenge.authorizer.external.ExternalDomain.{Account, AccountEvent, Transaction, TransactionEvent}
import io.nubank.challenge.authorizer.stores.AccountStore
import io.nubank.challenge.authorizer.window.ConcurrentWindow

sealed trait DomainValidation {
  def violationMessage: String
}

case object AccountAlreadyInit extends DomainValidation {
  override def violationMessage: String = "account-already-initialized"
}

case object AccountNotInit extends DomainValidation {
  override def violationMessage: String = "account-not-initialized"
}

case object CardInactive extends DomainValidation {
  override def violationMessage: String = "card-not-active"
}

case object InsufficientLimit extends DomainValidation {
  override def violationMessage: String = "insufficient-limit"
}

case object HighFreqTransaction extends DomainValidation {
  override def violationMessage: String = "high-frequency-small-interval"
}

case object DoubledTransaction extends DomainValidation {
  override def violationMessage: String = "doubled-transaction"
}

object DomainValidation {

  implicit val domainValidationCodec: Codec[DomainValidation] = deriveEnumerationCodec[DomainValidation]

  def validateAccount(account: Account)(implicit store: AccountStore): IO[ValidatedNec[DomainValidation, Account]] =
    for {
      acc <- store.get()
      valid <- acc match {
        case None      => IO.pure(account.validNec)
        case Some(acc) => IO.pure(AccountAlreadyInit.invalidNec)
      }
    } yield valid

  def validatedAccountActive(
      transaction: Transaction
  )(implicit store: AccountStore): IO[ValidatedNec[DomainValidation, Transaction]] =
    for {
      acc <- store.get()
      valid <- acc match {
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
      transaction: Transaction
  )(implicit store: AccountStore): IO[ValidatedNec[DomainValidation, Transaction]] =
    for {
      acc <- store.get()
      valid <- acc match {
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
      valid <- if (transactionCount <= 3) {
        IO.pure(transaction.validNec)
      } else {
        IO.pure(HighFreqTransaction.invalidNec)
      }
    } yield valid

  def validatedDoubledTransaction(
      transaction: Transaction
  )(implicit window: ConcurrentWindow): IO[ValidatedNec[DomainValidation, Transaction]] =
    for {
      transactionCount <- window.getWindow(transaction.merchant, transaction.amount)
      valid <- transactionCount match {
        case Some(entry) =>
          if (transactionCount.size <= 2) {
            IO.pure(transaction.validNec)
          } else {
            IO.pure(DoubledTransaction.invalidNec)
          }
        case None => IO.pure(transaction.validNec)
      }
    } yield valid

}
