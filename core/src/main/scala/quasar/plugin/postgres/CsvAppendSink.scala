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
import quasar.connector.destination.ResultSink.AppendSink
import quasar.connector.destination.{WriteMode => QWriteMode}
import quasar.connector.render.RenderConfig
import quasar.connector.{AppendEvent, DataEvent, MonadResourceErr, ResourceError}

import org.slf4s.Logging

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.syntax.bracket._
import cats.effect.{Effect, ExitCase, LiftIO, Timer}
import cats.implicits._

import doobie.free.connection.{commit, rollback, setAutoCommit, unit}
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.postgres.{PFCI, PFCM, PHC}
import doobie.util.transactor.Strategy
import doobie.{ConnectionIO, Fragment, Fragments, Transactor}

import fs2.{Chunk, Pipe, Stream}

import scala.concurrent.duration.MILLISECONDS

import skolems.∀

object CsvAppendSink extends Logging {
  def apply[F[_]: Effect: MonadResourceErr](
    xa0: Transactor[F],
    writeMode: WriteMode)(
    implicit timer: Timer[F])
      : AppendSink.Args[ColumnType.Scalar] => (RenderConfig[Byte], ∀[λ[α => Pipe[F, AppendEvent[Byte, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]) = {
    val strategy = Strategy(setAutoCommit(false), unit, rollback, unit)
    val xa = Transactor.strategy.modify(xa0, _ => strategy)

    run(xa, writeMode)
  }

  def run[F[_]: Effect: MonadResourceErr](
    xa: Transactor[F],
    writeMode: WriteMode)(
    args: AppendSink.Args[ColumnType.Scalar])(
    implicit timer: Timer[F])
      : (RenderConfig[Byte], ∀[λ[α => Pipe[F, AppendEvent[Byte, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]) = {
    val columns: NonEmptyList[Column[ColumnType.Scalar]] = args.columns

    val toConnectionIO = Effect.toIOK[F] andThen LiftIO.liftK[ConnectionIO]

    val table: F[Table] =
      tableFromPath(args.path) match {
        case Some(t) =>
          t.pure[F]
        case None =>
          MonadResourceErr[F].raiseError(ResourceError.notAResource(args.path))
      }

    def startLoad(tbl: Table, colSpecs: NonEmptyList[Fragment]): ConnectionIO[Unit] = {
      val indexColumn = args.pushColumns.primary map { c =>
        Fragments.parentheses(Fragment.const0(hygienicIdent(c.name)))
      }
      val mbCreateIndex = indexColumn.traverse_(createIndex(log)(tbl, _))

      (args.writeMode, writeMode) match {
        case (QWriteMode.Replace, WriteMode.Create) =>
          createTable(log)(tbl, colSpecs) >> mbCreateIndex >> commit
        case (QWriteMode.Replace, WriteMode.Replace) =>
          dropTableIfExists(log)(tbl) >> createTable(log)(tbl, colSpecs) >> mbCreateIndex >> commit
        case (QWriteMode.Replace, WriteMode.Truncate) =>
          createTableIfNotExists(log)(tbl, colSpecs) >> truncateTable(log)(tbl) >> mbCreateIndex >> commit
        case (QWriteMode.Replace, WriteMode.Append) =>
          createTableIfNotExists(log)(tbl, colSpecs) >> mbCreateIndex >> commit
        case (QWriteMode.Append, _) =>
          ().pure[ConnectionIO]
      }
    }

    def logEnd(startAt: Long, bytes: Ref[F, Long]): F[Unit] =
      for {
        endAt <- timer.clock.monotonic(MILLISECONDS)
        tbl <- table
        tbytes <- bytes.get
        _ <- debug[F](log)(s"SUCCESS: COPY ${tbytes} bytes to '${tbl}' in ${endAt - startAt} ms")
      } yield ()

    def eventHandler[A](totalBytes: Ref[F, Long])
        : Pipe[ConnectionIO, AppendEvent[Byte, OffsetKey.Actual[A]], Option[OffsetKey.Actual[A]]] = _.evalMap {
      case DataEvent.Create(records) =>
        handleCreate(totalBytes, records).as(none[OffsetKey.Actual[A]])
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

    def handleCommit[A](offset: OffsetKey.Actual[A]): ConnectionIO[OffsetKey.Actual[A]] =
      commit.as(offset)

    def pipe[A](events: Stream[F, AppendEvent[Byte, OffsetKey.Actual[A]]]): Stream[F, OffsetKey.Actual[A]] =
      Stream.force {
        for {
          byteCounter <- Ref[F].of(0L)
          tbl <- table
          colSpecs <- specifyColumnFragments[F](columns)
          startAt <- timer.clock.monotonic(MILLISECONDS)
          startLoad0 = Stream.eval(startLoad(tbl, colSpecs)).drain
          translated = events.translate(toConnectionIO)
          events0 = eventHandler(byteCounter)(translated).unNone
          rollback0 = Stream.eval(rollback).drain
          logEnd0 = Stream.eval(logEnd(startAt, byteCounter)).drain
        } yield startLoad0.transact(xa) ++ (events0 ++ rollback0).transact(xa) ++ logEnd0
      }
    (PostgresCsvConfig, ∀[λ[α => Pipe[F, AppendEvent[Byte, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]](pipe))
  }
}
