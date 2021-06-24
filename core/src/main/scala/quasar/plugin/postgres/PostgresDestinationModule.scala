/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.plugin.postgres

import slamdata.Predef._

import argonaut._, Argonaut._

import cats.data.EitherT
import cats.effect._
import cats.implicits._

import doobie._
import doobie.hikari.HikariTransactor
import doobie.implicits._

import java.util.concurrent.Executors

import org.slf4s.{Logging, LoggerFactory}

import quasar.api.destination.{DestinationError => DE, _}
import quasar.concurrent._
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Destination, DestinationModule, PushmiPullyu}
import quasar.lib.jdbc.destination.WriteMode

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NonFatal

object PostgresDestinationModule extends DestinationModule with Logging {

  type InitErr = DE.InitializationError[Json]

  // The duration to await validation of the initial connection.
  val ValidationTimeout: FiniteDuration = 10.seconds

  // Maximum number of database connections per-destination.
  val ConnectionPoolSize: Int = 32 // TODO make this customizable

  val destinationType: DestinationType = DestinationType("postgres", 1L)

  def sanitizeDestinationConfig(config: Json): Json =
    config.as[Config]
      .map(_.sanitized.asJson)
      .getOr(jEmptyObject)

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json,
      pushPull: PushmiPullyu[F])
      : Resource[F, Either[InitErr, Destination[F]]] = {

    val cfg0: Either[InitErr, Config] =
      config.as[Config].fold(
        (err, c) =>
          Left(DE.malformedConfiguration[Json, InitErr](
            destinationType,
            jString(Redacted),
            err)),

        Right(_))

    val validateConnection: ConnectionIO[Either[InitErr, Unit]] =
      FC.isValid(ValidationTimeout.toSeconds.toInt) map { v =>
        if (!v)
          Left(connectionInvalid(sanitizeDestinationConfig(config)))
        else
          Right(())
      }

    val init = for {
      cfg <- EitherT(cfg0.pure[Resource[F, ?]])

      suffix <- EitherT.right(Resource.eval(randomAlphaNum[F](6)))

      awaitPool <- EitherT.right(awaitConnPool[F](s"pgdest-await-$suffix", ConnectionPoolSize))

      xaPool <- EitherT.right(Blocker.cached[F](s"pgdest-transact-$suffix"))

      xa <- EitherT.right(hikariTransactor[F](cfg, awaitPool, xaPool))

      _ <- EitherT(Resource.eval(validateConnection.transact(xa) recover {
        case NonFatal(ex: Exception) =>
          Left(DE.connectionFailed[Json, InitErr](destinationType, sanitizeDestinationConfig(config), ex))
      }))

      _ <- EitherT.right[InitErr](Resource.eval(Sync[F].delay(
        log.info(s"Initialized postgres destination: tag = $suffix, config = ${cfg.sanitized.asJson}"))))

      logger <- EitherT.right[InitErr]{
        Resource.eval(Sync[F].delay(LoggerFactory(s"quasar.lib.destination.postgres-$suffix")))
      }

    } yield new PostgresDestination(
      xa,
      cfg.writeMode.getOrElse(WriteMode.Replace),
      cfg.schema,
      cfg.maxRetries,
      cfg.retryTransactionTimeout,
      logger): Destination[F]

    init.value
  }

  ////

  private val PostgresDriverFqcn: String = "org.postgresql.Driver"

  private def awaitConnPool[F[_]](name: String, size: Int)(implicit F: Sync[F])
      : Resource[F, ExecutionContext] = {

    val alloc =
      F.delay(Executors.newFixedThreadPool(size, NamedDaemonThreadFactory(name)))

    Resource.make(alloc)(es => F.delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor)
  }

  private def connectionInvalid(c: Json): InitErr =
    DE.connectionFailed[Json, InitErr](
      destinationType, c, new RuntimeException("Connection is invalid."))

  private def hikariTransactor[F[_]: Async: ContextShift](
      cfg: Config,
      connectPool: ExecutionContext,
      xaBlocker: Blocker)
      : Resource[F, HikariTransactor[F]] = {

    HikariTransactor.initial[F](connectPool, xaBlocker) evalMap { xa =>
      xa.configure { ds =>
        Sync[F] delay {
          ds.setJdbcUrl(jdbcUri(cfg.connectionUri))
          ds.setDriverClassName(PostgresDriverFqcn)
          ds.setMaximumPoolSize(ConnectionPoolSize)
          cfg.schema.foreach(ds.setSchema)
          xa
        }
      }
    }
  }
}
