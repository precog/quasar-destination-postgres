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
import cats.data._
import cats.effect.{Effect, Resource}
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.{Pipe, Stream}

import org.slf4s.Logging

import quasar.api.{Column, ColumnType}
import quasar.api.resource._
import quasar.connector._
import quasar.connector.render.RenderConfig
import quasar.lib.jdbc.destination.WriteMode

object CsvCreateSink extends Logging {
  def apply[F[_]: Effect: MonadResourceErr](
      xa: Transactor[F],
      writeMode: WriteMode,
      schema: Option[String])(
      path: ResourcePath,
      columns: NonEmptyList[Column[ColumnType.Scalar]])
      : (RenderConfig[Byte], Pipe[F, Byte, Unit]) = {

   val noopN: ConnectionIO ~> ConnectionIO = λ[ConnectionIO ~> ConnectionIO](x => x)

   def replaceR(flow: TempTableFlow): Resource[F, TempTableFlow] = {
      Resource.make(flow.pure[F])(x => flow.replace.transact(xa))
   }

   (PostgresCsvConfig, in => for {
     flow <- Stream.resource {
       TempTableFlow(xa, log, writeMode, path, schema, columns, None, None, noopN) flatMap replaceR
     }
     _ <- in.chunks.evalMap(c => flow.ingest(c).transact(xa))
   } yield ())
  }
}
