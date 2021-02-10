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
import quasar.connector.destination.{WriteMode => QWriteMode}
import quasar.connector.{MonadResourceErr, ResourceError}

import org.slf4s.Logging

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Effect, LiftIO, Timer}
import cats.implicits._

import doobie.free.connection.{commit, rollback}
import doobie.implicits._
import doobie.{ConnectionIO, Fragment, Fragments, Transactor}

import fs2.{Pipe, Stream}

import scala.concurrent.duration.MILLISECONDS

import skolems.∀

abstract class CsvSinkBuilder[F[_]: Effect: Timer: MonadResourceErr, Event[_]](
    xa: Transactor[F],
    writeMode: WriteMode,
    path: ResourcePath,
    idColumn: Option[Column[ColumnType.Scalar]],
    val columns: NonEmptyList[Column[ColumnType.Scalar]],
    qwriteMode: QWriteMode) extends Logging {

  def build: ∀[λ[α => Pipe[F, Event[OffsetKey.Actual[α]], OffsetKey.Actual[α]]]] =
    ∀[λ[α => Pipe[F, Event[OffsetKey.Actual[α]], OffsetKey.Actual[α]]]](pipe)

  private def pipe[A](events: Stream[F, Event[OffsetKey.Actual[A]]]): Stream[F, OffsetKey.Actual[A]] = {
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
  }

  protected val timer = Timer[F]

  protected val toConnectionIO = Effect.toIOK[F] andThen LiftIO.liftK[ConnectionIO]

  protected val table: F[Table] =
    tableFromPath(path) match {
      case Some(t) =>
        t.pure[F]
      case None =>
        MonadResourceErr[F].raiseError(ResourceError.notAResource(path))
    }

  private def startLoad(tbl: Table, colSpecs: NonEmptyList[Fragment]): ConnectionIO[Unit] = {
    val indexColumn = idColumn map { c =>
      Fragments.parentheses(Fragment.const0(hygienicIdent(c.name)))
    }
    val mbCreateIndex = indexColumn.traverse_(createIndex(log)(tbl, _))

    (qwriteMode, writeMode) match {
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

  private def logEnd(startAt: Long, bytes: Ref[F, Long]): F[Unit] =
    for {
      endAt <- timer.clock.monotonic(MILLISECONDS)
      tbl <- table
      tbytes <- bytes.get
      _ <- debug[F](log)(s"SUCCESS: COPY ${tbytes} bytes to '${tbl}' in ${endAt - startAt} ms")
    } yield ()

  def eventHandler[A](totalBytes: Ref[F, Long])
      : Pipe[ConnectionIO, Event[OffsetKey.Actual[A]], Option[OffsetKey.Actual[A]]]
}
