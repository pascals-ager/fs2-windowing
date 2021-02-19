package io.nubank.challenge.authorizer.events

import cats.effect.IO
import cats.effect.concurrent.Semaphore
import io.nubank.challenge.authorizer.external.ExternalDomain.{Account, AccountState, Transaction}
import io.nubank.challenge.authorizer.stores.AccountStoreService
import io.nubank.challenge.authorizer.validations.DomainValidation
import io.nubank.challenge.authorizer.validations.ValidationService.{
  validatedAccountActive,
  validatedAccountBalance,
  validatedDoubledTransaction,
  validatedTransactionFrequency
}
import io.nubank.challenge.authorizer.window.TransactionWindow

class TransactionsProcessor(store: AccountStoreService, window: TransactionWindow) {

  /**
    * Validate an TransactionEvent and apply it the AccountStore as well TransactionWindow
    * @param transaction: The transaction to be validated and applied to AccountStore and TransactionWindow
    * @return Returns state of the Account used in the transactions with any violations that may have occurred
    */
  def validateAndPutTransaction(
      transaction: Transaction
  )(implicit semaphore: Semaphore[IO]): IO[AccountState] =
    for {
      acq                      <- semaphore.acquire
      transactionAccountState  <- store.getAccount()
      acctActiveValidation     <- validatedAccountActive(transaction, transactionAccountState)
      acctBalanceValidation    <- validatedAccountBalance(transaction, transactionAccountState)
      transactFreqValidation   <- validatedTransactionFrequency(transaction)(window)
      transactDoubleValidation <- validatedDoubledTransaction(transaction)(window)
      validations <- IO.delay(
        Seq(acctActiveValidation, acctBalanceValidation, transactFreqValidation, transactDoubleValidation)
      )
      allValid  <- IO.delay(validations.forall(item => item.isValid))
      allErrors <- IO.delay(validations.flatMap(va => va.fold(l => l.toChain.toList, r => List[DomainValidation]())))
      acctState <- if (allValid) {
        for {
          /* transactionAccountState.get is safe because acctActiveValidation && acctBalanceValidation have checked against None */
          trans <- window.putTransaction(transaction)
          newAccState <- store.putAccount(
            Account(
              transactionAccountState.get.`active-card`,
              transactionAccountState.get.`available-limit` - transaction.amount
            )
          )
          acctState <- IO.delay(AccountState(newAccState, allErrors.toList))
        } yield acctState
      } else {
        IO.delay(AccountState(transactionAccountState, allErrors.toList))
      }
      rel <- semaphore.release
    } yield acctState
}
