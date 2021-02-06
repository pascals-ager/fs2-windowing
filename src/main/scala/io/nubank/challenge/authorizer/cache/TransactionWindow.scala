package io.nubank.challenge.authorizer.cache

import cats.effect.{IO, Resource}
import scalacache.{Entry, Mode}
import scalacache.guava._
import com.google.common.cache.{Cache, CacheBuilder}

import java.util.concurrent.TimeUnit

trait TransactionWindow {
  def put(key: String, value: Long): IO[Unit]
  def getSize: IO[Long]
  def violatesHigFreq(key: String, value: Long): IO[Boolean]
}

object TransactionWindow {
  implicit val mode: Mode[IO] = scalacache.CatsEffect.modes.async
  def create(expirationMins: Long): Resource[IO, TransactionWindow] = {

    val build: IO[GuavaCache[Long]] = IO.delay {
      val builder: Cache[String, Entry[Long]] = CacheBuilder
        .newBuilder()
        .expireAfterWrite(expirationMins, TimeUnit.MINUTES)
        .maximumSize(10000L)
        .build[String, Entry[Long]]

      new GuavaCache(builder)
    }

    Resource
      .makeCase(build) {
        case (gCache, _) =>
          IO.delay {
            gCache.underlying.cleanUp()
            gCache.close()
          }
      }
      .map { gCache =>
        new TransactionWindow {
          override def put(key: String, value: Long): IO[Unit] = IO.delay {
            gCache.put(key)(value)
          }

          override def getSize: IO[Long] = IO.delay {
            gCache.underlying.size()
          }

          override def violatesHigFreq(key: String, value: Long): IO[Boolean] = IO.delay {
            Option(gCache.underlying.getIfPresent(key)) match {
              case Some(entry) => (value == entry.value)
              case None        => false
            }
          }

        }
      }
  }
}
