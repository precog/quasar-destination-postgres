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
import quasar.connector.{MonadResourceErr, ResourceError, IdBatch}
import quasar.connector.destination.{WriteMode => QWriteMode}
import quasar.lib.jdbc.destination.WriteMode
import quasar.lib.jdbc.destination.flow.{Flow, FlowArgs}

import cats.Alternative
import cats.data.NonEmptyList
import cats.effect._
import cats.effect.concurrent.Ref
import cats.effect.syntax.bracket._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.free.connection.commit
import doobie.postgres.{PFCI, PFCM, PHC}
import doobie.postgres.implicits._

import fs2.Chunk

import org.slf4s.Logger

object TempTableFlow {
  def apply[F[_]: Sync: MonadResourceErr](
      xa: Transactor[F],
      logger: Logger,
      writeMode: WriteMode,
      schema: Option[String],
      args: FlowArgs[ColumnType.Scalar])
      : Resource[F, Flow[Byte]] = {

    def checkWriteMode(tableName: String, schemaName: Option[String]): F[Unit] = {
      val existing = checkExists(logger)(tableName, schemaName) map { results =>
        results.exists(_ === 1)
      }
      writeMode match {
        case WriteMode.Create => existing.transact(xa) flatMap { exists =>
          MonadResourceErr[F].raiseError(
            ResourceError.accessDenied(
              args.path,
              "Create mode is set but table exists already".some,
              none)).whenA(exists)
        }
        case _ =>
          ().pure[F]
      }
    }

    val acquire: F[(TempTable, Flow[Byte])] = for {
      tbl <- tableFromPath(args.path) match {
        case Some(t) => t.pure[F]
        case None => MonadResourceErr[F].raiseError(ResourceError.notAResource(args.path))
      }
      columnFragments <- specifyColumnFragments[F](args.columns)
      _ <- checkWriteMode(tbl, schema)
      totalBytes <- Ref.in[F, ConnectionIO, Long](0L)
      tempTable = TempTable(
        logger,
        totalBytes,
        writeMode,
        tbl,
        schema,
        args.columns,
        columnFragments,
        args.idColumn,
        args.filterColumn)
      _ <- {
        tempTable.drop >>
        tempTable.create >>
        commit
      }.transact(xa)
      refMode <- Ref.in[F, ConnectionIO, QWriteMode](args.writeMode)
    } yield {
      val flow = new Flow[Byte] {
        def delete(ids: IdBatch) =
          ().pure[ConnectionIO]

        def ingest(chunk: Chunk[Byte]): ConnectionIO[Unit] =
          tempTable.ingest(chunk) >> commit

        def replace: ConnectionIO[Unit] = refMode.get flatMap {
          case QWriteMode.Replace =>
            tempTable.persist >> commit >> refMode.set(QWriteMode.Append)
          case QWriteMode.Append =>
            append
        }
        def append: ConnectionIO[Unit] =
          tempTable.append >> commit
      }
      (tempTable, flow)
    }

    val release: ((TempTable, _)) => F[Unit] = { case (tempTable, _) =>
      (tempTable.drop >> commit).transact(xa)
    }

    Resource.make(acquire)(release).map(_._2)
  }

  private trait TempTable {
    def ingest(chunk: Chunk[Byte]): ConnectionIO[Unit]
    def drop: ConnectionIO[Unit]
    def create: ConnectionIO[Unit]
    def persist: ConnectionIO[Unit]
    def append: ConnectionIO[Unit]
  }

  private object TempTable {
    def apply(
        log: Logger,
        totalBytes: Ref[ConnectionIO, Long],
        writeMode: WriteMode,
        tableName: String,
        schemaName: Option[String],
        columns: NonEmptyList[Column[ColumnType.Scalar]],
        columnFragments: NonEmptyList[Fragment],
        idColumn: Option[Column[_]],
        filterColumn: Option[Column[_]])
        : TempTable = {
      val schemaFragment = schemaName map (x => Fragment.const0(hygienicIdent(x)))

      val tgtTableFragment = Fragment.const0(hygienicIdent(tableName))

      val tmpName = s"precog_temp_$tableName"
      val tmpTableFragment = Fragment.const0(hygienicIdent(tmpName))

      val tgtFragment = schemaFragment.fold(tgtTableFragment)(s => s ++ fr0"." ++ tgtTableFragment)
      val tmpFragment = schemaFragment.fold(tmpTableFragment)(s => s ++ fr0"." ++ tmpTableFragment)

      def recordChunks(c: Chunk[Byte]): ConnectionIO[Unit] =
        totalBytes.update(_ + c.size) >> (Sync[ConnectionIO].delay {
          log.trace(s"Sending ${c.size} bytes")
        })

      val indexName: String = s"precog_id_idx_$tableName"

      def createIndex(tbl: Fragment, col: Fragment): ConnectionIO[Unit] = {
        val fragment = fr"CREATE INDEX IF NOT EXISTS" ++ Fragment.const(hygienicIdent(indexName)) ++ fr"ON" ++
          tbl ++ fr0"(" ++ col ++ fr0")"
        fragment.updateWithLogHandler(logHandler(log)).run.void
      }

      def truncate: ConnectionIO[Unit] = {
        val fragment = fr"TRUNCATE TABLE" ++ tmpFragment
        fragment.updateWithLogHandler(logHandler(log)).run.void
      }

      def insertInto: ConnectionIO[Unit] = {
        val colFragments = columns.map { (col: Column[_]) =>
          Fragment.const0(hygienicIdent(col.name))
        }
        val allColumns = colFragments.intercalate(fr",")
        val toInsert = Fragments.parentheses(allColumns)

        val fragment = fr"INSERT INTO" ++ tgtFragment ++ fr0" " ++ toInsert ++ fr0" " ++
          fr"SELECT" ++ allColumns ++  fr" FROM" ++ tmpFragment

        fragment.updateWithLogHandler(logHandler(log)).run.void
      }

      def filterTempIds: ConnectionIO[Unit] = filterColumn traverse_ { (idColumn: Column[_]) =>
        val mkColumn: String => Fragment = parent =>
          Fragment.const0(parent) ++ fr0"." ++
          Fragment.const0(hygienicIdent(idColumn.name))

        val fragment =
          fr"DELETE FROM" ++ tgtFragment ++ fr"target" ++
          fr"USING" ++ tmpFragment ++ fr"temp" ++
          fr"WHERE" ++ mkColumn("target") ++ fr0"=" ++ mkColumn("temp")

        fragment.updateWithLogHandler(logHandler(log)).run.void
      }


      def createTgt: ConnectionIO[Unit] = {
        val fragment = fr"CREATE TABLE" ++ tgtFragment ++ Fragments.parentheses(columnFragments.intercalate(fr","))
        fragment.updateWithLogHandler(logHandler(log)).run.void
      }

      val createTgtIfNotExists: ConnectionIO[Unit] = {
        val fragment = fr"CREATE TABLE IF NOT EXISTS" ++ tgtFragment ++ Fragments.parentheses(columnFragments.intercalate(fr","))
        fragment.updateWithLogHandler(logHandler(log)).run.void
      }

      val truncateTgt: ConnectionIO[Unit] = {
        val fragment = fr"TRUNCATE" ++ tgtFragment
        fragment.updateWithLogHandler(logHandler(log)).run.void
      }

      val dropTgtIfExists: ConnectionIO[Unit] = {
        val fragment = fr"DROP TABLE IF EXISTS" ++ tgtFragment
        fragment.updateWithLogHandler(logHandler(log)).run.void
      }

      val mbCreateIndex: ConnectionIO[Unit] =
        (Alternative[Option].guard(idColumn.map(_.name) =!= filterColumn.map(_.name)) *> idColumn) traverse_
        { col =>
          val colFragment =
            Fragment.const(hygienicIdent(col.name))
          createIndex(tgtFragment, colFragment)
        }

      val mbCreateFilterIndex: ConnectionIO[Unit] =
        filterColumn.traverse_({ (col: Column[_]) =>
          val colFragment =
            Fragment.const(hygienicIdent(col.name))
          createIndex(tmpFragment, colFragment)
        })

      new TempTable {
        def ingest(chunk: Chunk[Byte]): ConnectionIO[Unit] = {
          val cols =
            columns.map(c => hygienicIdent(c.name)).intercalate(", ")

          val tbl = schemaName.fold("")(s => s"${hygienicIdent(s)}.") + hygienicIdent(tmpName)
          val copyQuery =
            s"COPY $tbl ($cols) FROM STDIN WITH (FORMAT csv, HEADER FALSE, ENCODING 'UTF8')"

          val data = chunk.toBytes

          val copied = PFCM.copyIn(copyQuery).bracketCase(
            PFCM.embed(_, PFCI.writeToCopy(data.values, data.offset, data.length))) { (pgci, exitCase) =>
            PFCM.embed(pgci, exitCase match {
              case ExitCase.Completed => PFCI.endCopy.void
              case _ => PFCI.isActive.ifM(PFCI.cancelCopy, PFCI.unit)
            })
          }

          for {
            back <- PHC.pgGetCopyAPI(copied)
            _ <- recordChunks(chunk)
          } yield back
        }

        def drop: ConnectionIO[Unit] = {
          val fragment = fr"DROP TABLE IF EXISTS" ++ tmpFragment
          fragment.updateWithLogHandler(logHandler(log)).run.void
        }

        def create: ConnectionIO[Unit] = {
          val fragment = fr"CREATE TABLE IF NOT EXISTS" ++ tmpFragment ++
            Fragments.parentheses(columnFragments.intercalate(fr","))

          fragment.updateWithLogHandler(logHandler(log)).run >> mbCreateFilterIndex
        }

        def rename: ConnectionIO[Unit] = {
          val fragment = fr"ALTER TABLE" ++ tmpFragment ++ fr" RENAME TO" ++ Fragment.const(hygienicIdent(tableName))
          fragment.updateWithLogHandler(logHandler(log)).run.void
        }

        def persist: ConnectionIO[Unit] = {
          val prepare = writeMode match {
            case WriteMode.Create =>
              createTgt >>
              mbCreateIndex >>
              insertInto >>
              truncate
            case WriteMode.Replace =>
              dropTgtIfExists >>
              rename >>
              create
            case WriteMode.Truncate =>
              createTgtIfNotExists >>
              truncateTgt >>
              insertInto >>
              truncate
            case WriteMode.Append =>
              createTgtIfNotExists >>
              insertInto >>
              truncate
          }
          prepare >> mbCreateIndex
        }

        def append: ConnectionIO[Unit] = {
          filterTempIds >>
          insertInto >>
          truncate
        }
      }
    }
  }
}


