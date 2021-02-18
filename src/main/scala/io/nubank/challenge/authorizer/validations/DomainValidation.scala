package io.nubank.challenge.authorizer.validations

import io.circe.generic.extras.semiauto.deriveEnumerationCodec
import io.circe.Codec

sealed trait DomainValidation {
  def violationMessage: String
}

case object `account-already-initialized` extends DomainValidation {
  override def violationMessage: String = "account-already-initialized"
}

case object `account-not-initialized` extends DomainValidation {
  override def violationMessage: String = "account-not-initialized"
}

case object `card-not-active` extends DomainValidation {
  override def violationMessage: String = "card-not-active"
}

case object `insufficient-limit` extends DomainValidation {
  override def violationMessage: String = "insufficient-limit"
}

case object `high-frequency-small-interval` extends DomainValidation {
  override def violationMessage: String = "high-frequency-small-interval"
}

case object `doubled-transaction` extends DomainValidation {
  override def violationMessage: String = "doubled-transaction"
}

object DomainValidation {
  implicit val domainValidationCodec: Codec[DomainValidation] = deriveEnumerationCodec[DomainValidation]
}
