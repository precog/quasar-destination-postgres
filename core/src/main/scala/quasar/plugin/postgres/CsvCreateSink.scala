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

import scala.concurrent.duration.MILLISECONDS

object CsvCreateSink extends Logging {
  def apply[F[_]: Effect: MonadResourceErr](
      xa: Transactor[F],
      writeMode: WriteMode,
      schema: Option[String])(
      dst: ResourcePath,
      columns: NonEmptyList[Column[ColumnType.Scalar]])(
      implicit timer: Timer[F])
      : (RenderConfig[Byte], Pipe[F, Byte, Unit]) = {

    val AE = ApplicativeError[F, Throwable]

    val table =
      tableFromPath(dst) match {
        case Some(t) =>
          t.pure[F]

        case None =>
          MonadResourceErr[F].raiseError(ResourceError.notAResource(dst))
      }

    val pipe: Pipe[F, Byte, Unit] = data => Stream.force(for {
      tbl <- table

      action = writeMode match {
        case WriteMode.Create => "Creating"
        case WriteMode.Replace => "Replacing"
        case WriteMode.Truncate => "Truncating"
        case WriteMode.Append => "Appending"
      }

      _ <- debug[F](log)(s"${action} '${tbl}'")

      // Telemetry
      totalBytes <- Ref[F].of(0L)
      startAt <- timer.clock.monotonic(MILLISECONDS)

      tempTbl = "precog_temp_" + tbl

      doCopyToTemp =
        data
          .chunks
          .evalTap(recordChunks[F](totalBytes, log))
          // TODO: Is there a better way?
          .translate(Effect.toIOK[F] andThen LiftIO.liftK[CopyManagerIO])
          .through(copyToTable(tempTbl, columns))

      colSpecs <- columns.traverse(columnSpec).fold(
        invalid => AE.raiseError(new ColumnTypesNotSupported(invalid)),
        _.pure[F])

      verifyExistence =
        writeMode match {
          case WriteMode.Create =>
            checkExists(log)(tbl, schema) flatMap { result =>
              if (result.exists(_ == 1))
                Sync[ConnectionIO].raiseError(
                  new TableAlreadyExists(tbl, schema)): ConnectionIO[Int]
              else
                0.pure[ConnectionIO]
            }
          case _ =>
            0.pure[ConnectionIO]
        }

      ensureTable =
        writeMode match {
          case WriteMode.Create =>
            createTable(log)(tbl, colSpecs)

          case WriteMode.Replace =>
            dropTableIfExists(log)(tbl) >> createTable(log)(tbl, colSpecs)

          case WriteMode.Truncate =>
            createTableIfNotExists(log)(tbl, colSpecs) >> truncateTable(log)(tbl)

          case WriteMode.Append =>
            createTableIfNotExists(log)(tbl, colSpecs)
        }

      ensureTempTable =
        dropTableIfExists(log)(tempTbl) >> createTable(log)(tempTbl, colSpecs)

      copy0 =
        Stream.eval(verifyExistence).void ++
          Stream.eval(ensureTempTable).void ++
          doCopyToTemp.translate(λ[FunctionK[CopyManagerIO, ConnectionIO]](PHC.pgGetCopyAPI(_))) ++
          Stream.eval(ensureTable).void ++
          Stream.eval(insertInto(log)(tempTbl, tbl)).void ++
          Stream.eval(dropTableIfExists(log)(tempTbl)).void

      copy = copy0.transact(xa) handleErrorWith { t =>
        Stream.eval(
          error[F](log)(s"COPY to '${tbl}' produced unexpected error: ${t.getMessage}", t) >>
            AE.raiseError(t))
      }

      logEnd = for {
        endAt <- timer.clock.monotonic(MILLISECONDS)
        tbytes <- totalBytes.get
        _ <- debug[F](log)(s"SUCCESS: COPY ${tbytes} bytes to '${tbl}' in ${endAt - startAt} ms")
      } yield ()


    } yield copy ++ Stream.eval(logEnd))

    (PostgresCsvConfig, pipe)
  }

  ////

  private def copyToTable(
      table: Table,
      columns: NonEmptyList[Column[ColumnType.Scalar]])
      : Pipe[CopyManagerIO, Chunk[Byte], Unit] = {
    val cols =
      columns
        .map(c => hygienicIdent(c.name))
        .intercalate(", ")

    val copyQuery =
      s"COPY ${hygienicIdent(table)} ($cols) FROM STDIN WITH (FORMAT csv, HEADER FALSE, ENCODING 'UTF8')"

    val logStart = debug[CopyManagerIO](log)(s"BEGIN COPY: `${copyQuery}`")

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
}
