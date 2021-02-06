package io.nubank.challenge.authorizer.external

object ExternalDomain {
  sealed trait ExternalEvent

  case class Account(`active-card`: String, `available-limit`: Int)
  case class Transaction(merchant: String, amount: Int, time: String)
  case class CreateAccount(account: Account)                extends ExternalEvent
  case class AuthorizeTransaction(transaction: Transaction) extends ExternalEvent
  case object Quit                                          extends ExternalEvent

}
