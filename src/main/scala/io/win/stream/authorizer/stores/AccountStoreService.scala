package io.win.stream.authorizer.stores

import cats.effect.concurrent.Ref
import cats.effect.{IO, Resource}
import io.win.stream.authorizer.external.ExternalDomain.Account
import org.typelevel.log4cats.Logger

trait AccountStoreService {

  /**
    * @return An Option[Account]: Option indication the Account may or may not exist
    */
  /* This signature ideally should have an account-uuid, but since we are dealing with a single account, this is
   * sufficient */
  def getAccount(): IO[Option[Account]]

  /**
    * @param account: The Account that must be inserted in to the store
    * @return The new state for the account
    */
  def putAccount(account: Account): IO[Option[Account]]

}

object AccountStoreService {

  /* Effectively models a single Account */
  def create()(implicit logger: Logger[IO]): Resource[IO, AccountStoreService] = {
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
