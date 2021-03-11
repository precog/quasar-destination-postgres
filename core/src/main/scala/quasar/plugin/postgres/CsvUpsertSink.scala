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
import quasar.connector.destination.ResultSink.UpsertSink
import quasar.connector.destination.{WriteMode => QWriteMode}
import quasar.connector.render.RenderConfig
import quasar.connector.{DataEvent, IdBatch, MonadResourceErr}

import org.slf4s.Logging

import cats.data.{NonEmptyList, NonEmptyVector}
import cats.effect.concurrent.Ref
import cats.effect.syntax.bracket._
import cats.effect.{Effect, ExitCase, Timer}
import cats.implicits._

import doobie.free.connection.{commit, rollback, setAutoCommit, unit}
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.postgres.{PFCI, PFCM, PHC}
import doobie.util.transactor.Strategy
import doobie.{ConnectionIO, Fragment, Fragments, Transactor}

import fs2.{Chunk, Pipe}

import skolems.∀

object CsvUpsertSink extends Logging {
  def apply[F[_]: Effect: Timer: MonadResourceErr](
      xa0: Transactor[F],
      writeMode: WriteMode)(
      args: UpsertSink.Args[ColumnType.Scalar])
      : (RenderConfig[Byte], ∀[λ[α => Pipe[F, DataEvent[Byte, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]) = {

    val strategy = Strategy(setAutoCommit(false), unit, rollback, unit)
    val xa = Transactor.strategy.modify(xa0, _ => strategy)

    (PostgresCsvConfig, builder(
      xa,
      writeMode,
      args.path,
      args.idColumn,
      args.otherColumns,
      args.writeMode).build)
  }

  def builder[F[_]: Effect: Timer: MonadResourceErr](
      xa: Transactor[F],
      writeMode: WriteMode,
      path: ResourcePath,
      idColumn: Column[ColumnType.Scalar],
      otherColumns: List[Column[ColumnType.Scalar]],
      qwriteMode: QWriteMode)
      : CsvSinkBuilder[F, DataEvent[Byte, *]] = new CsvSinkBuilder[F, DataEvent[Byte, *]](
      xa,
      writeMode,
      path,
      Some(idColumn),
      NonEmptyList(idColumn, otherColumns),
      qwriteMode) {

    def eventHandler[A](totalBytes: Ref[F, Long])
        : Pipe[ConnectionIO, DataEvent[Byte, OffsetKey.Actual[A]], Option[OffsetKey.Actual[A]]] = _.evalMap {
      case DataEvent.Create(records) =>
        handleCreate(totalBytes, records).as(none[OffsetKey.Actual[A]])
      case DataEvent.Delete(recordIds) =>
        handleDelete(recordIds).as(none[OffsetKey.Actual[A]])
      case DataEvent.Commit(offset) =>
        handleCommit(offset).map(_.some)
    }

    def handleCreate(totalBytes: Ref[F, Long], records: Chunk[Byte]): ConnectionIO[Unit] =
      for {
        tbl <- toConnectionIO(table)

        colSpecs <- toConnectionIO(specifyColumnFragments[F](columns))

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

    def handleDelete(recordIds: IdBatch): ConnectionIO[Unit] =
      if (recordIds.size === 0) {
        ().pure[ConnectionIO]
      } else {

        def preambleFragment(tbl: Table): Fragment =
          fr"DELETE FROM" ++
            Fragment.const(hygienicIdent(tbl)) ++
            fr"WHERE" ++
            Fragment.const(hygienicIdent(idColumn.name))

        def deleteFrom(preamble: Fragment): ConnectionIO[Int] =
          recordIds match {
            case IdBatch.Strings(values, size) =>
              Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector)) // trust size passed by quasar
                .updateWithLogHandler(logHandler(log))
                .run
            case IdBatch.Longs(values, size) =>
              Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector)) // trust size passed by quasar
                .updateWithLogHandler(logHandler(log))
                .run
            case IdBatch.Doubles(values, size) =>
              Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector)) // trust size passed by quasar
                .updateWithLogHandler(logHandler(log))
                .run
            case IdBatch.BigDecimals(values, size) =>
              Fragments.in(preamble, NonEmptyVector.fromVectorUnsafe(values.take(size).toVector)) // trust size passed by quasar
                .updateWithLogHandler(logHandler(log))
                .run
          }

        for {
          tbl <- toConnectionIO(table)
          preamble = preambleFragment(tbl)
          _ <- deleteFrom(preamble)
        } yield ()
      }

    def handleCommit[I](offset: OffsetKey.Actual[I]): ConnectionIO[OffsetKey.Actual[I]] =
      commit.as(offset)
  }
}
