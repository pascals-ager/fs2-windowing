package io.nubank.challenge.authorizer.internal

object BusinessDomain {
  sealed trait BusinessEvent
  case object AccountNotInit        extends BusinessEvent
  case object CardNotActive         extends BusinessEvent
  case object HighFreqSmallInterval extends BusinessEvent

}
