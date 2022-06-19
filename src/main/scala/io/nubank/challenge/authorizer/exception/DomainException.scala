package io.win.stream.authorizer.exception

object DomainException {
  case class UnrecognizedEventException(msg: String) extends Exception
  case class ParsingFailureException(msg: String)    extends Exception
  case class DecodingFailureException(msg: String)   extends Exception
}
