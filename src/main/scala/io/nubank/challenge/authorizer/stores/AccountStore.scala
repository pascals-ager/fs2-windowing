package io.nubank.challenge.authorizer.stores

import cats.effect.{IO, Resource}
import io.nubank.challenge.authorizer.external.ExternalDomain.Account

import scala.util.Try

trait AccountStore {
  /* This signature ideally should have an account-uuid, but since we are dealing with a single account, this is
   * sufficient */
  def get(): IO[Option[Account]]
  def put(account: Account): IO[Unit]
  def remove(account: Account): IO[Unit]
}

object AccountStore {

  def create(): Resource[IO, AccountStore] = {
    val open = IO.delay {
      scala.collection.mutable.Set[Account]()
    }
    Resource
      .makeCase(open) {
        case (store, _) =>
          IO.delay {
            store.clear()
          }
      }
      .map { store =>
        new AccountStore {
          /* The implementation is simple because I assume one account */
          override def get(): IO[Option[Account]] = IO.delay(Try(store.head).toOption)

          override def put(account: Account): IO[Unit] =
            for {
              oldState <- get()
              _ <- oldState match {
                case Some(old) =>
                  for {
                    _ <- remove(old)
                    _ <- IO.delay(store += account)
                  } yield ()
                case None => IO.delay(store += account)
              }
            } yield ()

          override def remove(account: Account): IO[Unit] = IO.delay(store.clear())

        }

      }

  }
}
