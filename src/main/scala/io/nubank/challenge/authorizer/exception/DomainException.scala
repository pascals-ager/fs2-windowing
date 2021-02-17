package io.nubank.challenge.authorizer.exception

object DomainException {
  case class UnrecognizedEventType(msg: String)    extends Exception
  case class ParsingFailureException(msg: String)  extends Exception
  case class DecodingFailureException(msg: String) extends Exception
}
