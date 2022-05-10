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

import scala._, Predef._

import cats.data.NonEmptyList
import cats.effect.{Effect, Timer, Resource}

import doobie.Transactor

import quasar.api.ColumnType
import quasar.api.destination._
import quasar.connector.MonadResourceErr
import quasar.connector.destination._
import quasar.lib.jdbc.destination.WriteMode
import quasar.lib.jdbc.destination.flow.{FlowSinks, FlowArgs, Flow, Retry}

import org.slf4s.Logger

import scala.concurrent.duration._

final class PostgresDestination[F[_]: Effect: MonadResourceErr: Timer](
    xa: Transactor[F],
    writeMode: WriteMode,
    schema: Option[String],
    maxRetries: Int,
    retryTimeout: FiniteDuration,
    logger: Logger,
    writeChunkSize: Int = PostgresDestination.DefaultWriteChunkSize)
    extends LegacyDestination[F] with FlowSinks[F, ColumnType.Scalar, Byte] {

  val destinationType: DestinationType =
    PostgresDestinationModule.destinationType

  def flowResource(args: FlowArgs[ColumnType.Scalar]): Resource[F, Flow[Byte]] =
    TempTableFlow(xa, logger, writeMode, schema, args) map { (f: Flow[Byte]) =>
      f.mapK(Retry[F](maxRetries, retryTimeout))
    }

  def render(args: FlowArgs[ColumnType.Scalar]) = PostgresCsvConfig

  val flowTransactor = xa
  val flowLogger = logger

  val sinks: NonEmptyList[ResultSink[F, Type]] =
    flowSinks.map(RechunkingSink[F, Type](writeChunkSize))
}

object PostgresDestination {
  // Prefer to write chunks of at least 32MiB
  val DefaultWriteChunkSize: Int = 32 * 1024 * 1024
}
