package io.nubank.challenge.authorizer.validations

sealed trait DomainValidation {
  def violationMessage: String
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
