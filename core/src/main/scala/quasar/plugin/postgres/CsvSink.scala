/*
 * Copyright 2014–2019 SlamData Inc.
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

import slamdata.Predef.{Stream => _, _}

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
import doobie.util.log.{ExecFailure, ProcessingFailure, Success}

import fs2.{Chunk, Pipe, Stream}

import java.lang.IllegalArgumentException

import org.slf4s.Logging

import quasar.connector._
import quasar.api.resource._
import quasar.api.table.{ColumnType, TableColumn}

import scala.concurrent.duration.MILLISECONDS

import shims._

object CsvSink extends Logging {
  def apply[F[_]: Effect: MonadResourceErr](
      xa: Transactor[F],
      writeMode: WriteMode)(
      dst: ResourcePath,
      columns: List[TableColumn],
      data: Stream[F, Byte])(
      implicit timer: Timer[F])
      : Stream[F, Unit] = {

    val AE = ApplicativeError[F, Throwable]

    val table =
      tableFromPath(dst) match {
        case Some(t) =>
          t.pure[F]

        case None =>
          MonadResourceErr[F].raiseError(ResourceError.notAResource(dst))
      }

    val tableColumns =
      columns.toNel match {
        case Some(cols) =>
          cols.pure[F]

        case None =>
          AE.raiseError(new IllegalArgumentException("No columns specified."))
      }

    for {
      tbl <- Stream.eval(table)

      cols <- Stream.eval(tableColumns)

      action = writeMode match {
        case WriteMode.Create => "Creating"
        case WriteMode.Replace => "Replacing"
      }

      _ <- Stream.eval(debug[F](s"${action} '${tbl}' with schema ${cols.show}"))

      // Telemetry
      totalBytes <- Stream.eval(Ref[F].of(0L))
      startAt <- Stream.eval(timer.clock.monotonic(MILLISECONDS))

      doCopy =
        data
          .chunks
          .evalTap(recordChunks[F](totalBytes))
          // TODO: Is there a better way?
          .translate(Effect.toIOK[F] andThen LiftIO.liftK[CopyManagerIO])
          .through(copyToTable(tbl, cols))

      colSpecs <- Stream.eval(
        cols.traverse(columnSpec).fold(
          invalid => AE.raiseError(new ColumnTypesNotSupported(invalid)),
          _.pure[F]))

      ensureTable =
        writeMode match {
          case WriteMode.Create =>
            createTable(tbl, colSpecs)

          case WriteMode.Replace =>
            dropTableIfExists(tbl) >> createTable(tbl, colSpecs)
        }

      doCopy0 =
        Stream.eval(ensureTable) >> doCopy.translate(λ[FunctionK[CopyManagerIO, ConnectionIO]](PHC.pgGetCopyAPI(_)))

      _ <- doCopy0.translate(λ[FunctionK[ConnectionIO, F]](_.transact(xa))) handleErrorWith { t =>
        Stream.eval(error[F](s"COPY to '${tbl}' produced unexpected error: ${t.getMessage}", t) >> AE.raiseError(t))
      }

      endAt <- Stream.eval(timer.clock.monotonic(MILLISECONDS))
      tbytes <- Stream.eval(totalBytes.get)

      _ <- Stream.eval(debug[F](s"SUCCESS: COPY ${tbytes} bytes to '${tbl}' in ${endAt - startAt} ms"))

    } yield ()
  }

  ////

  private val logHandler: LogHandler =
    LogHandler {
      case Success(q, _, e, p) =>
        log.debug(s"SUCCESS: `$q` in ${(e + p).toMillis}ms (${e.toMillis} ms exec, ${p.toMillis} ms proc)")

      case ExecFailure(q, _, e, t) =>
        log.debug(s"EXECUTION_FAILURE: `$q` after ${e.toMillis} ms, detail: ${t.getMessage}", t)

      case ProcessingFailure(q, _, e, p, t) =>
        log.debug(s"PROCESSING_FAILURE: `$q` after ${(e + p).toMillis} ms (${e.toMillis} ms exec, ${p.toMillis} ms proc (failed)), detail: ${t.getMessage}", t)
    }

  private def error[F[_]: Sync](msg: => String, cause: => Throwable): F[Unit] =
    Sync[F].delay(log.error(msg, cause))

  private def debug[F[_]: Sync](msg: => String): F[Unit] =
    Sync[F].delay(log.debug(msg))

  private def trace[F[_]: Sync](msg: => String): F[Unit] =
    Sync[F].delay(log.trace(msg))

  private def logChunkSize[F[_]: Sync](c: Chunk[Byte]): F[Unit] =
    trace[F](s"Sending ${c.size} bytes")

  private def recordChunks[F[_]: Sync](total: Ref[F, Long])(c: Chunk[Byte]): F[Unit] =
    total.update(_ + c.size) >> logChunkSize[F](c)

  private def copyToTable(table: Table, columns: NonEmptyList[TableColumn]): Pipe[CopyManagerIO, Chunk[Byte], Unit] = {
    val cols =
      columns
        .map(c => hygienicIdent(c.name))
        .intercalate(", ")

    val copyQuery =
      s"COPY ${hygienicIdent(table)} ($cols) FROM STDIN WITH (FORMAT csv, HEADER FALSE, ENCODING 'UTF8')"

    val logStart = debug[CopyManagerIO](s"BEGIN COPY: `${copyQuery}`")

    val startCopy =
      Stream.bracketCase(PFCM.copyIn(copyQuery) <* logStart) { (pgci, exitCase) =>
        PFCM.embed(pgci, exitCase match {
          case ExitCase.Completed => PFCI.endCopy.void
          case _ => PFCI.isActive.ifM(PFCI.cancelCopy, PFCI.unit)
        })
      }

    in => startCopy flatMap { pgci =>
      in.map(_.toBytes) evalMap { bs =>
        PFCM.embed(pgci, PFCI.writeToCopy(bs.values, bs.offset, bs.length))
      }
    }
  }

  private def createTable(table: Table, colSpecs: NonEmptyList[Fragment]): ConnectionIO[Int] = {
    val preamble =
      fr"CREATE TABLE" ++ Fragment.const(hygienicIdent(table))

    (preamble ++ Fragments.parentheses(colSpecs.intercalate(fr",")))
      .updateWithLogHandler(logHandler)
      .run
  }

  private def dropTableIfExists(table: Table): ConnectionIO[Int] =
    (fr"DROP TABLE IF EXISTS" ++ Fragment.const(hygienicIdent(table)))
      .updateWithLogHandler(logHandler)
      .run

  private def columnSpec(tc: TableColumn): ValidatedNel[ColumnType.Scalar, Fragment] =
    pgColumnType(tc.tpe).map(Fragment.const(hygienicIdent(tc.name)) ++ _)

  private val pgColumnType: ColumnType.Scalar => ValidatedNel[ColumnType.Scalar, Fragment] = {
    case ColumnType.Null => fr0"smallint".validNel
    case ColumnType.Boolean => fr0"boolean".validNel
    case ColumnType.LocalTime => fr0"time".validNel
    case ColumnType.OffsetTime => fr0"time with timezone".validNel
    case ColumnType.LocalDate => fr0"date".validNel
    case t @ ColumnType.OffsetDate => t.invalidNel
    case ColumnType.LocalDateTime => fr0"timestamp".validNel
    case ColumnType.OffsetDateTime => fr0"timestamp with time zone".validNel
    case ColumnType.Interval => fr0"interval".validNel
    case ColumnType.Number => fr0"numeric".validNel
    case ColumnType.String => fr0"text".validNel
  }
}
