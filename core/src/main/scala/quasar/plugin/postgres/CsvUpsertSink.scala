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
import quasar.api.{Column, ColumnType}
import quasar.connector.destination.ResultSink.UpsertSink
import quasar.connector.{DataEvent, IdBatch, MonadResourceErr, ResourceError}

import org.slf4s.Logging

import cats.data.{NonEmptyList, NonEmptyVector}
import cats.effect.concurrent.Ref
import cats.effect.syntax.bracket._
import cats.effect.{Async, Effect, ExitCase, LiftIO, Timer}
import cats.implicits._

import doobie.free.connection.{rollback, setAutoCommit, unit}
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.postgres.{CopyManagerIO, CopyInIO, PFCI, PFCM, PHC}
import doobie.util.transactor.Strategy
import doobie.{ConnectionIO, FC, Fragment, Fragments, Transactor}

import fs2.{Chunk, Pipe, Stream}

import skolems.Forall

object CsvUpsertSink extends Logging {
  def apply[F[_]: Effect: MonadResourceErr](
    xa0: Transactor[F],
    writeMode: WriteMode)(
    implicit timer: Timer[F])
      : Forall[λ[α => UpsertSink.Args[F, ColumnType.Scalar, α] => Stream[F, OffsetKey.Actual[α]]]] = {

    val strategy = Strategy(setAutoCommit(false), unit, rollback, unit)
    val xa = Transactor.strategy.modify(xa0, _ => strategy)

    Forall[λ[α => UpsertSink.Args[F, ColumnType.Scalar, α] => Stream[F, OffsetKey.Actual[α]]]](run(xa))
  }

  def run[F[_]: Effect: MonadResourceErr, I](
    xa: Transactor[F])(
    args: UpsertSink.Args[F, ColumnType.Scalar, I])(
    implicit timer: Timer[F])
      : Stream[F, OffsetKey.Actual[I]] = {

    val columns: NonEmptyList[Column[ColumnType.Scalar]] =
      NonEmptyList(args.idColumn, args.otherColumns)

    val toConnectionIO = Effect.toIOK[F] andThen LiftIO.liftK[ConnectionIO]

    val append = false

    val table: F[Table] =
      tableFromPath(args.path) match {
        case Some(t) =>
          t.pure[F]

        case None =>
          MonadResourceErr[F].raiseError(ResourceError.notAResource(args.path))
      }


    def handleCreate(totalBytes: Ref[F, Long], records: Chunk[Byte]): ConnectionIO[Unit] =
      for {
        tbl <- toConnectionIO(table)

        colSpecs <- toConnectionIO(specifyColumnFragments[F](columns))

        indexColumn = Fragments.parentheses(Fragment.const(hygienicIdent(args.idColumn.name)))

        _ <- if (append) dropTableIfExists(log)(tbl) else ().pure[ConnectionIO]

        _ <- createTable(log)(tbl, colSpecs) >> createIndex(log)(tbl, indexColumn)

        cols = columns.map(c => hygienicIdent(c.name)).intercalate(", ")

        copyQuery =
          s"COPY ${hygienicIdent(tbl)} ($cols) FROM STDIN WITH (FORMAT csv, HEADER FALSE, ENCODING 'UTF8')"

        data = records.toBytes

        copied = PFCM.copyIn(copyQuery).bracketCase(PFCM.embed(_, PFCI.writeToCopy(data.values, data.offset, data.length))) { (pgci, exitCase) =>

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
        val values: NonEmptyVector[Fragment] =
          recordIds match {
            case IdBatch.Strings(values, _) =>
              NonEmptyVector
                .fromVectorUnsafe(values.toVector) // trust the size passed from quasar
                .map(str => singleQuoted(str.show)) // TODO: prevent injection heru

            case IdBatch.Longs(values, _) =>
              NonEmptyVector
                .fromVectorUnsafe(values.toVector)
                .map(l => singleQuoted(l.show))

            case IdBatch.Doubles(values, _) =>
              NonEmptyVector
                .fromVectorUnsafe(values.toVector)
                .map(d => singleQuoted(d.show))

            case IdBatch.BigDecimals(values, _) =>
              NonEmptyVector
                .fromVectorUnsafe(values.toVector)
                .map(bd => singleQuoted(bd.show))
          }

        for {
          tbl <- toConnectionIO(table)
          colSpec = Fragment.const(hygienicIdent(args.idColumn.name))
          _ <- deleteFrom(tbl, colSpec, values)
        } yield ()
      }

    def handleCommit(offset: OffsetKey.Actual[I]): ConnectionIO[OffsetKey.Actual[I]] =
      FC.commit.as(offset)

    def singleQuoted(str: String): Fragment =
      fr0"'" ++ Fragment.const0(str) ++ fr0"'"

    def deleteFrom(table: Table, col: Fragment, values: NonEmptyVector[Fragment])
        : ConnectionIO[Int] = {
      val preamble =
        fr"DELETE FROM" ++ Fragment.const(hygienicIdent(table)) ++ fr"WHERE" ++ col

      val set = values.intercalate(fr",")
      val condition = fr"IN" ++ fr0"(" ++ set ++ fr0")"

      val deleteQuery = preamble ++ condition

      println(deleteQuery)

      (preamble ++ condition)
        .updateWithLogHandler(logHandler(log))
        .run
    }

    def eventHandler(totalBytes: Ref[F, Long])
        : Pipe[ConnectionIO, DataEvent[OffsetKey.Actual[I]], Option[OffsetKey.Actual[I]]] =
      _ evalMap {
        case DataEvent.Create(records) =>
          handleCreate(totalBytes, records).as(none[OffsetKey.Actual[I]])
        case DataEvent.Delete(recordIds) =>
          handleDelete(recordIds).as(none[OffsetKey.Actual[I]])
        case DataEvent.Commit(offset) =>
          handleCommit(offset).map(_.some)
      }

    Stream.eval(Ref[F].of(0L)) flatMap { tb =>
      val translated: Stream[ConnectionIO, DataEvent[OffsetKey.Actual[I]]] =
        args.input.translate(toConnectionIO)

      eventHandler(tb)(translated).unNone.transact(xa)
    }
  }
}
