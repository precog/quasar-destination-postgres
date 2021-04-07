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

import cats._
import cats.arrow.FunctionK
import cats.data._
import cats.effect.{Effect, ExitCase, LiftIO, Sync, Timer}
import cats.effect.concurrent.Ref
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.postgres._
import doobie.postgres.implicits._

import fs2.{Chunk, Pipe, Stream}

import org.slf4s.Logging

import quasar.api.{Column, ColumnType}
import quasar.api.resource._
import quasar.connector._
import quasar.connector.render.RenderConfig
import quasar.lib.jdbc.destination.WriteMode

import scala.concurrent.duration.MILLISECONDS

object CsvCreateSink extends Logging {
  def apply[F[_]: Effect: MonadResourceErr](
      xa: Transactor[F],
      writeMode: WriteMode,
      schema: Option[String])(
      path: ResourcePath,
      columns: NonEmptyList[Column[ColumnType.Scalar]])(
      implicit timer: Timer[F])
      : (RenderConfig[Byte], Pipe[F, Byte, Unit]) = {

   val noopN: ConnectionIO ~> ConnectionIO = λ[ConnectionIO ~> ConnectionIO](x => x)

   (PostgresCsvConfig, in => for {
     flow <- Stream.resource(TempTableFlow(xa, log, writeMode, path, schema, columns, None, None, noopN))
     _ <- in.chunks.evalMap(x => flow.ingest(x).transact(xa))
     _ <- Stream.eval(flow.replace.transact(xa))
   } yield ())
  }
}
