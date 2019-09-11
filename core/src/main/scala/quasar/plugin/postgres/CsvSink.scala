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
import cats.data._
import cats.effect.{Effect, ExitCase, LiftIO}
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.postgres._
import doobie.postgres.implicits._

import fs2.{Pipe, Stream}

import quasar.connector._
import quasar.api.resource._
import quasar.api.table.{ColumnType, TableColumn}

import java.lang.IllegalArgumentException

object CsvSink {
  def apply[F[_]: Effect: MonadResourceErr](
      xa: Transactor[F],
      writeMode: WriteMode)(
      dst: ResourcePath,
      columns: List[TableColumn],
      data: Stream[F, Byte])
      : F[Unit] = {

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
          ApplicativeError[F, Throwable]
            .raiseError(new IllegalArgumentException("No columns specified."))
      }

    (table product tableColumns) flatMap { case (tbl, cols) =>
      val doCopy =
        data
          // TODO: Is there a better way?
          .translate(Effect.toIOK[F] andThen LiftIO.liftK[CopyManagerIO])
          .through(copyToTable(tbl, cols))
          .compile
          .drain

      val ensureTable =
        writeMode match {
          case WriteMode.Create =>
            createTable(tbl, cols)

          case WriteMode.Replace =>
            dropTableIfExists(tbl) >> createTable(tbl, cols)
        }

      (ensureTable >> PHC.pgGetCopyAPI(doCopy)).transact(xa)
    }
  }

  ////

  private type Ident = String
  private type Table = Ident

  private def copyToTable(table: Table, columns: NonEmptyList[TableColumn]): Pipe[CopyManagerIO, Byte, Unit] = {
    val cols =
      columns
        .map(c => hygenicIdent(c.name))
        .intercalate(", ")

    val copyQuery =
      s"COPY ${hygenicIdent(table)} ($cols) FROM STDIN WITH (FORMAT csv, HEADER FALSE, ENCODING 'UTF8')"

    val startCopy =
      Stream.bracketCase(PFCM.copyIn(copyQuery)) { (pgci, exitCase) =>
        PFCM.embed(pgci, exitCase match {
          case ExitCase.Completed => PFCI.endCopy.void
          case _ => PFCI.isActive.ifM(PFCI.cancelCopy, PFCI.unit)
        })
      }

    in => startCopy flatMap { pgci =>
      in.chunks.map(_.toBytes) evalMap { bs =>
        PFCM.embed(pgci, PFCI.writeToCopy(bs.values, bs.offset, bs.length))
      }
    }
  }

  private def createTable(table: Table, columns: NonEmptyList[TableColumn]): ConnectionIO[Int] = {
    val preamble =
      fr"CREATE TABLE" ++ Fragment.const(hygenicIdent(table))

    val colSpecs =
      columns
        .map(c => Fragment.const(hygenicIdent(c.name)) ++ pgColumnType(c.tpe))
        .intercalate(fr",")

    (preamble ++ Fragments.parentheses(colSpecs)).update.run
  }

  private def dropTableIfExists(table: Table): ConnectionIO[Int] =
    (fr"DROP TABLE IF EXISTS" ++ Fragment.const(hygenicIdent(table))).update.run

  private def hygenicIdent(ident: Ident): Ident =
    s""""${ident.replace("\"", "\"\"")}""""

  private val pgColumnType: ColumnType.Scalar => Fragment = {
    case ColumnType.Null => fr0"smallint"
    case ColumnType.Boolean => fr0"boolean"
    case ColumnType.LocalTime => fr0"time"
    case ColumnType.OffsetTime => fr0"time with timezone"
    case ColumnType.LocalDate => fr0"date"
    // TODO: Can this be improved?
    case ColumnType.OffsetDate => fr0"text"
    case ColumnType.LocalDateTime => fr0"timestamp"
    case ColumnType.OffsetDateTime => fr0"timestamp with time zone"
    case ColumnType.Interval => fr0"interval"
    case ColumnType.Number => fr0"numeric"
    case ColumnType.String => fr0"text"
  }

  private def tableFromPath(p: ResourcePath): Option[Table] =
    Some(p) collect {
      case table /: ResourcePath.Root => table
    }
}
