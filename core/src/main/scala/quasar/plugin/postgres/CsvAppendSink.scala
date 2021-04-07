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

import quasar.api.push.OffsetKey
import quasar.api.resource.ResourcePath
import quasar.api.{Column, ColumnType}
import quasar.connector.destination.ResultSink.AppendSink
import quasar.connector.destination.{WriteMode => QWriteMode}
import quasar.connector.render.RenderConfig
import quasar.connector.{AppendEvent, DataEvent, MonadResourceErr}
import quasar.lib.jdbc.destination.WriteMode

import org.slf4s.Logging

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.syntax.bracket._
import cats.effect.{Effect, ExitCase, Timer}
import cats.implicits._

import doobie.free.connection.{commit, rollback, setAutoCommit, unit}
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.postgres.{PFCI, PFCM, PHC}
import doobie.util.transactor.Strategy
import doobie.{ConnectionIO, Transactor}

import fs2.{Chunk, Pipe}

import skolems.∀

object CsvAppendSink extends Logging {
  def apply[F[_]: Effect: Timer: MonadResourceErr](
      xa0: Transactor[F],
      writeMode: WriteMode)(
      args: AppendSink.Args[ColumnType.Scalar])
      : (RenderConfig[Byte], ∀[λ[α => Pipe[F, AppendEvent[Byte, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]) = {
    val strategy = Strategy(setAutoCommit(false), unit, rollback, unit)
    val xa = Transactor.strategy.modify(xa0, _ => strategy)

    (PostgresCsvConfig, builder(
      xa,
      writeMode,
      args.path,
      args.pushColumns.primary,
      args.columns,
      args.writeMode).build)
  }

  def builder[F[_]: Effect: Timer: MonadResourceErr](
      xa: Transactor[F],
      writeMode: WriteMode,
      path: ResourcePath,
      idColumn: Option[Column[ColumnType.Scalar]],
      columns: NonEmptyList[Column[ColumnType.Scalar]],
      qwriteMode: QWriteMode)
      : CsvSinkBuilder[F, AppendEvent[Byte, *]] =
    new CsvSinkBuilder[F, AppendEvent[Byte, *]](xa, writeMode, path, idColumn, columns, qwriteMode) { self =>
      def eventHandler[A](totalBytes: Ref[F, Long])
          : Pipe[ConnectionIO, AppendEvent[Byte, OffsetKey.Actual[A]], Option[OffsetKey.Actual[A]]] = _.evalMap {
        case DataEvent.Create(records) =>
          handleCreate(totalBytes, records).as(none[OffsetKey.Actual[A]])
        case DataEvent.Commit(offset) =>
          handleCommit(offset).map(_.some)
      }

      private def handleCreate(totalBytes: Ref[F, Long], records: Chunk[Byte]): ConnectionIO[Unit] =
        for {
          tbl <- self.toConnectionIO(table)
          colSpecs <- self.toConnectionIO(specifyColumnFragments[F](columns))
          cols = columns.map(c => hygienicIdent(c.name)).intercalate(", ")
          copyQuery =
            s"COPY ${hygienicIdent(tbl)} ($cols) FROM STDIN WITH (FORMAT csv, HEADER FALSE, ENCODING 'UTF8')"
          data = records.toBytes
          copied = PFCM.copyIn(copyQuery).bracketCase(
            PFCM.embed(_, PFCI.writeToCopy(data.values, data.offset, data.length))) { (pgci, exitCase) =>
              PFCM.embed(pgci, exitCase match {
                case ExitCase.Completed => PFCI.endCopy.void
                case _ => PFCI.isActive.ifM(PFCI.cancelCopy, PFCI.unit)
              })
            }
          back <- PHC.pgGetCopyAPI(copied)

          _ <- toConnectionIO(recordChunks[F](totalBytes, log)(records))

        } yield back

      private def handleCommit[A](offset: OffsetKey.Actual[A]): ConnectionIO[OffsetKey.Actual[A]] =
        commit.as(offset)
    }
}
