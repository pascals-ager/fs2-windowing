package io.nubank.challenge.authorizer.validations

import io.circe.generic.extras.semiauto.deriveEnumerationCodec
import io.circe.Codec

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
}
