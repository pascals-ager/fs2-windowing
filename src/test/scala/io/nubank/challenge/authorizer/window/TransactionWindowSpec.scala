package io.nubank.challenge.authorizer.window

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.Ref
import org.scalatest.funspec.AnyFunSpec
import fs2._
import _root_.io.nubank.challenge.authorizer.external.ExternalDomain.Transaction
import cats.implicits._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class TransactionWindowSpec extends AnyFunSpec {

  //val window: Stream[IO, Ref[IO, Resource[IO, TransactionWindow]]] = Stream.eval(Ref[IO].of(TransactionWindow.create(2)))

  /*  val a = for {
    win <- Stream.eval(Ref[IO].of(TransactionWindow.create(2)))
    ref <- Stream.eval(win.get)

  } yield ()*/

  implicit val timer: Timer[IO]     = IO.timer(ExecutionContext.global)
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  //val transaction: Transaction = Transaction("Nike", 240, 1581256283, System.currentTimeMillis())

  val outer: Stream[IO, Option[Seq[(Long, Long)]]] = Stream
    .resource(TransactionWindow.create(120.seconds))
    .flatMap { window =>
      val a: IO[Option[Seq[(Long, Long)]]] = for {
        ts          <- IO.pure(System.currentTimeMillis())
        _           <- IO.delay(println(s"Using ${ts}"))
        transaction <- IO.pure(Transaction("Nike", 240, 1581256283, ts))
        put         <- window.put(transaction)
        get         <- window.get("Nike", 240)
      } yield get

      val b = Stream.eval(window.evictExpiredTimestamps)
      Stream.eval(a).concurrently(b)
    }

  outer.evalTap(l => IO.delay(println(l.toString()))).compile.drain.unsafeRunSync()
  outer.delayBy(20.seconds).evalTap(l => IO.delay(println(l.toString()))).compile.drain.unsafeRunSync()

}
