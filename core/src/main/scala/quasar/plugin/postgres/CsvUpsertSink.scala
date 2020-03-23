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

import quasar.api.{Column, ColumnType}
import quasar.connector.{DataEvent, MonadResourceErr, Offset, ResourceError}
import quasar.connector.destination.ResultSink.UpsertSink

import cats.arrow.FunctionK
import cats.data.NonEmptyList
import cats.effect.{Bracket, Effect, ExitCase, LiftIO, Timer}
import cats.implicits._

import doobie.{ConnectionIO, FC, Fragment, Transactor}
import doobie.free.connection.{rollback, setAutoCommit, unit}
import doobie.implicits._
import doobie.postgres.{CopyManagerIO, PFCI, PFCM, PHC}
import doobie.postgres.implicits._
import doobie.util.transactor.Strategy

import fs2.{Chunk, Pipe, Stream}

import org.postgresql.copy.CopyIn

import org.slf4s.Logging

import skolems.Forall

// final case class Args[F[_], T, A](
//     path: ResourcePath,
//     columns: NonEmptyList[Column[T]],
//     correlationId: Column[TypedKey[T, A]],
//     input: Stream[F, DataEvent.Primitive[A, Offset]])
// }
//
// type Offset = Column[Exists[ActualKey]]

object CsvUpsertSink extends Logging {

  // ResourcePath is used to extract the table name with `tableFromPath`
  // create Stream[F, Byte] and columns are passed to `copyToTable`
  // then we `createTable` with this information

  def apply[F[_]: Effect: MonadResourceErr](
      xa: Transactor[F])(
      implicit timer: Timer[F]) // TODO telemetry
      : Forall[λ[α => UpsertSink.Args[F, ColumnType.Scalar, α] => Stream[F, Offset]]] = {

    val strategy = Strategy(setAutoCommit(false), unit, rollback, unit)
    val modified = Transactor.strategy.modify(xa, _ => strategy)

    Forall[λ[α => UpsertSink.Args[F, ColumnType.Scalar, α] => Stream[F, Offset]]](run(modified))
  }

  def run[F[_]: Effect: MonadResourceErr, I](
      xa: Transactor[F])(
      args: UpsertSink.Args[F, ColumnType.Scalar, I])(
      implicit timer: Timer[F])
      : Stream[F, Offset] = {

    // TODO use updated Args (Append or Replace)
    val append: Boolean = ???

    val table: F[Table] =
      tableFromPath(args.path) match {
        case Some(t) =>
          t.pure[F]

        case None =>
          MonadResourceErr[F].raiseError(ResourceError.notAResource(args.path))
      }

    // TODO use updated Args.columns
    val columns: NonEmptyList[Column[ColumnType.Scalar]] =
      args.correlationId.map(_.value.getConst) :: args.columns

    val toConnectionIO = (Effect.toIOK[F] andThen LiftIO.liftK[ConnectionIO])

    def handleCreate(records: Chunk[Byte]): ConnectionIO[Unit] =
      for {
        tbl <- toConnectionIO(table)

        colSpecs <- toConnectionIO(specifyColumnFragments[F](columns))
        colSpec <- toConnectionIO(specifyColumnFragment[F](args.correlationId.map(_.value.getConst)))

        _ <- if (append) dropTableIfExists(log)(tbl) else ().pure[ConnectionIO]

        _ <- createTable(log)(tbl, colSpecs) >> createIndex(log)(tbl, colSpec)

        copyQuery =
          s"COPY ${hygienicIdent(tbl)} ($columns) FROM STDIN WITH (FORMAT csv, HEADER FALSE, ENCODING 'UTF8')"

        copy = (Bracket[CopyManagerIO, Throwable].bracketCase[CopyIn, CopyIn](PFCM.copyIn(copyQuery))(_.pure[CopyManagerIO]) { (pgci, exitCase) =>
          PFCM.embed(pgci, exitCase match {
            case ExitCase.Completed => PFCI.endCopy.void
            case _ => PFCI.isActive.ifM(PFCI.cancelCopy, PFCI.unit)
          })
        })

        data = records.toBytes

        copied = copy.flatMap(pgci =>
          PFCM.embed(pgci, PFCI.writeToCopy(data.values, data.offset, data.length)))

        back <- (λ[FunctionK[CopyManagerIO, ConnectionIO]](PHC.pgGetCopyAPI(_))).apply(copied)
      } yield back

    def handleDelete(recordIds: NonEmptyList[I]): ConnectionIO[Unit] = {
      val key = args.correlationId.tpe

      val values: NonEmptyList[Fragment] =
        recordIds map { id =>
          Fragment.const(key.reify.coerce(id).toString) // TODO do we really want `.toString` here?
        }

      for {
        tbl <- toConnectionIO(table)
        colSpec <- toConnectionIO(specifyColumnFragment[F](args.correlationId.map(_.value.getConst)))
        _ <- deleteFrom(log)(tbl, colSpec, values)
      } yield ()
    }

    def handleCommit(offset: Offset): ConnectionIO[Offset] =
      FC.commit.map(_ => offset)

    val eventHandler: Pipe[ConnectionIO, DataEvent.Primitive[I, Offset], Option[Offset]] =
      _ evalMap {
        case DataEvent.Create(records) =>
          handleCreate(records).map(_ => None: Option[Offset])
        case DataEvent.Delete(recordIds) =>
          handleDelete(recordIds).map(_ => None: Option[Offset])
        case DataEvent.Commit(offset) =>
          handleCommit(offset).map(Some(_): Option[Offset])
      }

    val translated: Stream[ConnectionIO, DataEvent.Primitive[I, Offset]] =
      args.input.translate(Effect.toIOK[F] andThen LiftIO.liftK[ConnectionIO])

    eventHandler(translated).unNone.transact(xa)
  }
}
