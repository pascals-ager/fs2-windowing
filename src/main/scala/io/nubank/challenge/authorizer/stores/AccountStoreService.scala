package io.nubank.challenge.authorizer.stores

import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{IO, Resource}
import io.nubank.challenge.authorizer.external.ExternalDomain.{Account, Transaction}

import scala.collection.mutable
import scala.util.Try

trait AccountStoreService {
  /* This signature ideally should have an account-uuid, but since we are dealing with a single account, this is
   * sufficient */
  def getAccount(): IO[Option[Account]]
  def putAccount(account: Account): IO[Option[Account]]

}

object AccountStoreService {

  /* Effectively models a single Account */
  def create(): Resource[IO, AccountStoreService] = {
    val open: IO[Ref[IO, Option[Account]]] = Ref[IO].of {
      None
    }
    Resource
      .makeCase(open) {
        case (store, _) =>
          for {
            _ <- store.update(acc => None)
          } yield ()
      }
      .map { store =>
        new AccountStoreService {
          /* The implementation is simple because I assume one account */
          override def getAccount(): IO[Option[Account]] =
            for {
              acc <- store.get
            } yield acc

          override def putAccount(account: Account): IO[Option[Account]] =
            for {
              newState <- store.updateAndGet { acc =>
                {
                  Some(account)
                }
              }
            } yield newState

        }

      }

  }
}
