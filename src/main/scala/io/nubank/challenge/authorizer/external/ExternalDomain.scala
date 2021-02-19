package io.nubank.challenge.authorizer.external

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.nubank.challenge.authorizer.validations.DomainValidation

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

object ExternalDomain {
  sealed trait ExternalEvent

  case class Account(`active-card`: Boolean, `available-limit`: Int)
  case class Transaction(merchant: String, amount: Int, transactionTime: Long, processingTime: Long)
  case class AccountState(account: Option[Account], violations: List[DomainValidation])

  case object Start                                     extends ExternalEvent
  case class AccountEvent(account: Account)             extends ExternalEvent
  case class TransactionEvent(transaction: Transaction) extends ExternalEvent

  implicit val accountDecoder: Decoder[Account] = deriveDecoder[Account]
  implicit val accountEncoder: Encoder[Account] = deriveEncoder[Account]

  implicit val accountEventDecoder: Decoder[AccountEvent] = deriveDecoder[AccountEvent]
  implicit val accountEventEncoder: Encoder[AccountEvent] = deriveEncoder[AccountEvent]

  implicit val accountStateDecoder: Decoder[AccountState] = deriveDecoder[AccountState]
  implicit val accountStateEncoder: Encoder[AccountState] = deriveEncoder[AccountState]

  implicit val transactionEventDecoder: Decoder[TransactionEvent] = deriveDecoder[TransactionEvent]
  implicit val transactionEventEncoder: Encoder[TransactionEvent] = deriveEncoder[TransactionEvent]

  implicit val transactionEncoder: Encoder[Transaction] = deriveEncoder[Transaction]

  implicit val transactionDecoder: Decoder[Transaction] = new Decoder[Transaction] {
    override def apply(c: HCursor): Result[Transaction] =
      for {
        merchant <- c.downField("merchant").as[String]
        amount   <- c.downField("amount").as[Int]
        transactionTime <- c
          .downField("time")
          .as[String]
          .map(time => OffsetDateTime.parse(time, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toEpochSecond)
      } yield new Transaction(merchant, amount, transactionTime, System.currentTimeMillis())
  }
}
