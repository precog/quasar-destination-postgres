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

import argonaut._, Argonaut._, ArgonautScalaz._

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.implicits.legacy.localdate._
import doobie.util.Read

import fs2.{Pull, Stream}

import java.net.URI
import java.time._

import org.specs2.matcher.MatchResult
import org.specs2.execute.AsResult
import org.specs2.specification.core.{Fragment => SFragment}

import qdata.time._

import quasar.EffectfulQSpec
import quasar.api.{Column, ColumnType}
import quasar.api.destination._
import quasar.api.push.OffsetKey
import quasar.api.resource._
import quasar.contrib.scalaz.MonadError_
import quasar.connector._
import quasar.connector.destination.{WriteMode => QWriteMode, _}
import quasar.lib.jdbc.destination.WriteMode

import scala.Float
import scala.concurrent.ExecutionContext.Implicits.global

import scalaz.-\/
import scalaz.syntax.show._

import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}
import shapeless.ops.record.{Keys, Values}
import shapeless.record._
import shapeless.syntax.singleton._

import shims.applicativeToScalaz

object PostgresDestinationSpec extends EffectfulQSpec[IO] with CsvSupport with PGTemporalMeta {

  sequential

  val B = TemporalBounds

  "initialization" should {
    "fail with malformed config when not decodable" >>* {
      val cfg = Json("malformed" := true)

      dest(cfg)(r => IO.pure(r match {
        case Left(DestinationError.MalformedConfiguration(_, c, _)) =>
          c must_=== jString(Redacted)

        case _ => ko("Expected a malformed configuration")
      }))
    }

    "fail when unable to connect to database" >>* {
      val cfg = config(url = "postgresql://localhost:1234/foobar")

      dest(cfg)(r => IO.pure(r match {
        case Left(DestinationError.ConnectionFailed(_, c, _)) =>
          c must_=== cfg

        case _ => ko("Expected a connection failed")
      }))
    }
  }

  "seek sinks (upsert and append)" should {
    "write after commit" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)
        }
    }
  }
  /*
    "write two chunks with a single commit" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Create(List(
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)

          offsets must_== List(OffsetKey.Actual.string("commit1"))
        }
    }

    "not write without a commit" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Create(List(("x" ->> "quz") :: ("y" ->> "corge") :: HNil)),
          UpsertEvent.Commit("commit1"),
          UpsertEvent.Create(List(("x" ->> "baz") :: ("y" ->> "qux") :: HNil)))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "quz" :: "corge" :: HNil)
          offsets must_== List(OffsetKey.Actual.string("commit1"))
        }
    }

    "commit twice in a row" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Commit("commit1"),
          UpsertEvent.Commit("commit2"))

      for {
        tbl <- freshTableName
        (values, offsets) <- consumer(tbl, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List("foo" :: "bar" :: HNil)
          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
    }

    "upsert delete rows with string typed primary key" >>* {
      Consumer.upsert[String :: String :: HNil](config(), TestConnectionUrl).use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.StringIds(List("foo"))),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List("baz" :: "qux" :: HNil)
          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
      }
    }

    "upsert delete rows with long typed primary key" >>* {
      Consumer.upsert[Int :: String :: HNil](config(), TestConnectionUrl).use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> 40) :: ("y" ->> "bar") :: HNil,
                ("x" ->> 42) :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.LongIds(List(40))),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(Column("x", ColumnType.Number)), QWriteMode.Replace, events)
        } yield {
          values must_== List(42 :: "qux" :: HNil)
          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
      }
    }

    "upsert empty deletes without failing" >>* {
      Consumer.upsert[String :: String :: HNil](config(), TestConnectionUrl).use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.StringIds(List())),
            UpsertEvent.Commit("commit2"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "baz" :: "qux" :: HNil)

          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"))
        }
      }
    }

    "upsert delete same id twice" >>* {
      Consumer.upsert[String :: String :: HNil](config(), TestConnectionUrl).use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(
              List(
                ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
                ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
            UpsertEvent.Commit("commit1"),
            UpsertEvent.Delete(Ids.StringIds(List("foo"))),
            UpsertEvent.Commit("commit2"),
            UpsertEvent.Delete(Ids.StringIds(List("foo"))),
            UpsertEvent.Commit("commit3"))

        for {
          tbl <- freshTableName
          (values, offsets) <- consumer(tbl, Some(Column("x", ColumnType.String)), QWriteMode.Replace, events)
        } yield {
          values must_== List("baz" :: "qux" :: HNil)

          offsets must_== List(
            OffsetKey.Actual.string("commit1"),
            OffsetKey.Actual.string("commit2"),
            OffsetKey.Actual.string("commit3"))
        }
      }
    }

    "creates table and then appends" >> appendAndUpsert[String :: String :: HNil] { (toOpt, consumer) =>
      val events1 =
        Stream(
          UpsertEvent.Create(List(("x" ->> "foo") :: ("y" ->> "bar") :: HNil)),
          UpsertEvent.Commit("commit1"))

      val events2 =
        Stream(
          UpsertEvent.Create(List(("x" ->> "bar") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit2"))

      for {
        tbl <- freshTableName

        _ <- consumer(tbl, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events1)

        (values, _) <- consumer(tbl, Some(Column("x", ColumnType.String)), QWriteMode.Append, events2)
        } yield {
          values must_== List(
            "foo" :: "bar" :: HNil,
            "bar" :: "qux" :: HNil)
        }
    }

    "creates an index for each table on correlation id" >> idOnly[String :: String :: HNil] { (toOpt, consumer) =>
      val events =
        Stream(
          UpsertEvent.Create(List(
            ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
            ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

      for {
        tblA <- freshTableName
        tblB <- freshTableName

        _ <- consumer(tblA, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events)

        _ <- consumer(tblB, toOpt(Column("x", ColumnType.String)), QWriteMode.Replace, events)

        tables = NonEmptyList.of(tblA, tblB)

        checkIndexes =
          fr"SELECT count(*) FROM pg_indexes WHERE" ++ Fragments.in(fr"tablename", tables)

        indexCount <- runDb[IO, Int](checkIndexes.query[Int].unique)
      } yield {
        indexCount must_=== 2
      }
    }
    "doesn't create indices if there is no ids" >>*
      Consumer.append[String :: String :: HNil](config(), TestConnectionUrl).use { consumer =>
        val events =
          Stream(
            UpsertEvent.Create(List(
              ("x" ->> "foo") :: ("y" ->> "bar") :: HNil,
              ("x" ->> "baz") :: ("y" ->> "qux") :: HNil)),
          UpsertEvent.Commit("commit1"))

        for {
          tblA <- freshTableName
          tblB <- freshTableName

          _ <- consumer(tblA, None, QWriteMode.Replace, events)

          _ <- consumer(tblB, None, QWriteMode.Replace, events)

          tables = NonEmptyList.of(tblA, tblB)

          checkIndexes =
            fr"SELECT count(*) FROM pg_indexes WHERE" ++ Fragments.in(fr"tablename", tables)

          indexCount <- runDb[IO, Int](checkIndexes.query[Int].unique)
        } yield {
          indexCount must_=== 0
        }
      }
  }

  "csv sink" should {
    "reject empty paths with NotAResource" >>* {
      csv(config()) { sink =>
        val p = ResourcePath.root()
        val (_, pipe) = sink.consume(p, NonEmptyList.one(Column("a", ColumnType.Boolean)))
        val r = pipe(Stream.empty).compile.drain

        MRE.attempt(r).map(_ must beLike {
          case -\/(ResourceError.NotAResource(p2)) => p2 must_=== p
        })
      }
    }

    "reject paths with > 1 segments with NotAResource" >>* {
      csv(config()) { sink =>
        val p = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
        val (_, pipe) = sink.consume(p, NonEmptyList.one(Column("a", ColumnType.Boolean)))
        val r = pipe(Stream.empty).compile.drain

        MRE.attempt(r).map(_ must beLike {
          case -\/(ResourceError.NotAResource(p2)) => p2 must_=== p
        })
      }
    }

    "reject OffsetDate columns" >>* {
      val MinOffsetDate = OffsetDate(LocalDate.MIN, ZoneOffset.MIN)
      val MaxOffsetDate = OffsetDate(LocalDate.MAX, ZoneOffset.MAX)

      freshTableName flatMap { table =>
        val cfg = config(url = TestConnectionUrl)
        val rs = Stream(("min" ->> MinOffsetDate) :: ("max" ->> MaxOffsetDate) :: HNil)

        csv(cfg)(drainAndSelectAs[IO, String :: String :: HNil](TestConnectionUrl, table, _, rs))
          .attempt
          .map(_ must beLeft.like {
            case ColumnTypesNotSupported(ts) => ts.toList must contain(ColumnType.OffsetDate: ColumnType)
          })
      }
    }

    "quote table names to prevent injection" >>* {
      csv(config()) { sink =>
        val recs = List(("x" ->> 2.3) :: ("y" ->> 8.1) :: HNil)

        drainAndSelect(
          TestConnectionUrl,
          "foobar; drop table really_important; create table haha",
          sink,
          Stream.emits(recs)
        ).map(_ must_=== recs)
      }
    }

    "support table names containing double quote" >>* {
      csv(config()) { sink =>
        val recs = List(("x" ->> 934.23) :: ("y" ->> 1234424.1239847) :: HNil)

        drainAndSelect(
          TestConnectionUrl,
          """the "table" name""",
          sink,
          Stream.emits(recs)
        ).map(_ must_=== recs)
      }
    }

    "quote column names to prevent injection" >>* {
      mustRoundtrip(("from nowhere; drop table super_mission_critical; select *" ->> 42) :: HNil)
    }

    "support columns names containing a double quote" >>* {
      mustRoundtrip(("""the "column" name""" ->> 76) :: HNil)
    }

    "create an empty table when input is empty" >>* {
      csv(config()) { sink =>
        type R = Record.`"a" -> String, "b" -> Int, "c" -> LocalDate`.T

        for {
          tbl <- freshTableName
          r <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream.empty.covaryAll[IO, R])
        } yield r must beEmpty
      }
    }

    "create a row for each line of input" >>* mustRoundtrip(
      ("foo" ->> 1) :: ("bar" ->> "baz1") :: ("quux" ->> 34.34234) :: HNil,
      ("foo" ->> 2) :: ("bar" ->> "baz2") :: ("quux" ->> 35.34234) :: HNil,
      ("foo" ->> 3) :: ("bar" ->> "baz3") :: ("quux" ->> 36.34234) :: HNil)

    "overwrite any existing table with the same name with WriteMode.Replace" >>* {
      csv(config(writeMode = WriteMode.Replace)) { sink =>
        val r1 = ("x" ->> 1) :: ("y" ->> "two") :: ("z" ->> 3.00001) :: HNil
        val r2 = ("a" ->> "b") :: ("c" ->> "d") :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r1))
          res2 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r2))
        } yield (res1 must_=== List(r1)) and (res2 must_=== List(r2))
      }
    }

    "overwrite any existing table with the same name with WriteMode.Truncate" >>* {
      csv(config(writeMode = WriteMode.Truncate)) { sink =>
        val r1 = ("x" ->> 1) :: ("y" ->> "two") :: ("z" ->> 3.00001) :: HNil
        val r2 = ("x" ->> 2) :: ("y" ->> "skldfj") :: ("z" ->> 3.00002) :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r1))
          res2 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r2))
        } yield (res1 must_=== List(r1)) and (res2 must_=== List(r2))
      }
    }

    "append to existing table with WriteMode.Append" >>* {
      csv(config(writeMode = WriteMode.Append)) { sink =>
        val r1 = ("x" ->> 1) :: ("y" ->> "two") :: ("z" ->> 3.00001) :: HNil
        val r2 = ("x" ->> 2) :: ("y" ->> "skldfj") :: ("z" ->> 3.00002) :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r1))
          res2 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r2))
        } yield (res1 must_=== List(r1)) and (res2 must_=== List(r1) ++ List(r2))
      }
    }

    "create new table with WriteMode.Create" >>* {
      csv(config(writeMode = WriteMode.Create)) { sink =>
        val r1 = ("x" ->> 1) :: ("y" ->> "two") :: ("z" ->> 3.00001) :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r1))
        } yield (res1 must_=== List(r1))
      }
    }

    "create new table in user-defined schema with WriteMode.Create" >>* {
      val schema = "myschema"

      csv(config(schema = Some(schema), writeMode = WriteMode.Create)) { sink =>
        val r1 = ("x" ->> 1) :: ("y" ->> "two") :: ("z" ->> 3.00001) :: HNil

        for {
          tbl <- freshTableName
          res1 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r1), schema)
        } yield (res1 must_=== List(r1))
      }
    }

    "error when table already exists in public schema with WriteMode.Create" >>* {
      val action = csv(config(writeMode = WriteMode.Create)) { sink =>
        val r1 = ("x" ->> 1) :: ("y" ->> "two") :: ("z" ->> 3.00001) :: HNil
        val r2 = ("a" ->> "b") :: ("c" ->> "d") :: HNil

        for {
          tbl <- freshTableName
          _ <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r1))
          res2 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r2))
        } yield ()
      }

      action
        .attempt
        .map(_ must beLeft.like {
          case TableAlreadyExists(_, _) => ok
        })
    }

    "error when table already exists in user-provided schema with WriteMode.Create" >>* {
      val schema = "myschema"

      val action = csv(config(schema = Some(schema), writeMode = WriteMode.Create)) { sink =>
        val r1 = ("x" ->> 1) :: ("y" ->> "two") :: ("z" ->> 3.00001) :: HNil
        val r2 = ("a" ->> "b") :: ("c" ->> "d") :: HNil

        for {
          tbl <- freshTableName
          _ <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r1), schema)
          res2 <- drainAndSelect(TestConnectionUrl, tbl, sink, Stream(r2), schema)
        } yield ()
      }

      action
        .attempt
        .map(_ must beLeft.like {
          case TableAlreadyExists(_, _) => ok
        })
    }

    "roundtrip Boolean" >>* mustRoundtrip(("min" ->> true) :: ("max" ->> false) :: HNil)
    "roundtrip Short" >>* mustRoundtrip(("min" ->> Short.MinValue) :: ("max" ->> Short.MaxValue) :: HNil)
    "roundtrip Int" >>* mustRoundtrip(("min" ->> Int.MinValue) :: ("max" ->> Int.MaxValue) :: HNil)
    "roundtrip Long" >>* mustRoundtrip(("min" ->> Long.MinValue) :: ("max" ->> Long.MaxValue) :: HNil)
    "roundtrip Float" >>* mustRoundtrip(("min" ->> Float.MinValue) :: ("max" ->> Float.MaxValue) :: HNil)
    "roundtrip Double" >>* mustRoundtrip(("min" ->> Double.MinValue) :: ("max" ->> Double.MaxValue) :: HNil)
    "roundtrip BigDecimal" >>* mustRoundtrip(("value" ->> BigDecimal(Long.MaxValue).pow(5)) :: HNil)

    "roundtrip String" >>* {
      IO(scala.util.Random.nextString(32)) flatMap { s =>
        mustRoundtrip(("value" ->> s) :: HNil)
      }
    }

    "roundtrip empty string as NULL" >>* {
      freshTableName flatMap { table =>
        val cfg = config(url = TestConnectionUrl)
        val rs = Stream(("value" ->> "") :: HNil)

        csv(cfg)(drainAndSelectAs[IO, Option[String]](TestConnectionUrl, table, _, rs))
          .map(_ must_=== List(None))
      }
    }
    "roundtrip LocalTime" >>* mustRoundtrip(
      ("min" ->> B.MinLocalTime) ::
      ("max" ->> B.MaxLocalTime) ::
      HNil)
    */
/*  TODO: Figure out how to represent this through the PG driver
    "roundtrip OffsetTime" >>* mustRoundtrip(
      ("min" ->> B.MinOffsetTime) ::
      ("max" ->> B.MaxOffsetTime) ::
      HNil)
*/
/*
    "load LocalDate bounds" >>* {
      loadAndRetrieve(("min" ->> B.MinLocalDate) :: ("max" ->> B.MaxLocalDate) :: HNil)
        .map(_ must not(beEmpty))
    }

    "roundtrip LocalDate (driver limited)" >>* mustRoundtrip(
      // Sigh, though it inserts/stores them correctly, the PG driver seems
      // to return any LocalDates BCE as CE, so artificially limiting this to CE.
      ("min" ->> LocalDate.of(1, 1, 1)) ::
      ("max" ->> B.MaxLocalDate) ::
      HNil)

    "roundtrip LocalDateTime" >>* mustRoundtrip(
      ("min" ->> B.MinLocalDateTime) ::
      ("max" ->> B.MaxLocalDateTime) ::
      HNil)

    "load OffsetDateTime bounds" >>* {
      loadAndRetrieve(
        ("min" ->> B.MinOffsetDateTime) ::
        ("max" ->> B.MaxOffsetDateTime) ::
        HNil)
        .map(_ must not(beEmpty))
    }

    "roundtrip OffsetDateTime (driver limited)" >>* {
      val r =
        // All dates prior to 1582 are returned by the driver shifted some days,
        // maybe due to old leap-year corrections? So limit to after that.
        ("min" ->> B.MinOffsetDateTime.plusYears(4713).plusYears(1582)) ::
        ("max" ->> B.MaxOffsetDateTime) ::
        HNil

      // PG stores everything as UTC and OffsetDateTime considers the zone
      // in equality, so normalize results to Instant
      object inst extends Poly1 {
        implicit val onlyCase = at[OffsetDateTime](_.toInstant)
      }

      loadAndRetrieve(r) map { out =>
        out.map(_.map(inst)) must containTheSameElementsAs(List(r.values.map(inst)))
      }
    }

    "roundtrip DateTimeInterval" >>* mustRoundtrip(
      ("neg" ->> DateTimeInterval.make(-23, -1, -15, -7234, -67000000)) ::
      ("zero" ->> DateTimeInterval.zero) ::
      ("pos" ->> DateTimeInterval.make(3, 1, 14, 600, 342000)) ::
      HNil)

    */
  //}

  val DM = PostgresDestinationModule

  val TestConnectionUrl: String = "postgresql://localhost:5433/postgres?user=postgres&password=secret"

  implicit val CS: ContextShift[IO] = IO.contextShift(global)

  implicit val TM: Timer[IO] = IO.timer(global)

  implicit val MRE: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  object renderRow extends renderForCsv(PostgresCsvConfig)

  val freshTableName: IO[String] =
    randomAlphaNum[IO](6).map(suffix => s"pgtest_test_${suffix}")

  def runDb[F[_]: Async: ContextShift, A](fa: ConnectionIO[A], uri: String = TestConnectionUrl): F[A] =
    fa.transact(Transactor.fromDriverManager[F]("org.postgresql.Driver", jdbcUri(new URI(uri))))

  def config(url: String = TestConnectionUrl, schema: Option[String] = None, writeMode: WriteMode = WriteMode.Replace): Json =
    ("connectionUri" := url) ->:
    ("schema" := schema) ->:
    ("writeMode" := writeMode) ->:
    jEmptyObject

  def csv[A](cfg: Json)(f: ResultSink.CreateSink[IO, ColumnType.Scalar, Byte] => IO[A]): IO[A] =
    dest(cfg) {
      case Left(err) =>
        IO.raiseError(new RuntimeException(err.shows))

      case Right(dst) =>
        dst.sinks.toList
          .collectFirst { case c @ ResultSink.CreateSink(_) => c }
          .map(s => f(s.asInstanceOf[ResultSink.CreateSink[IO, ColumnType.Scalar, Byte]]))
          .getOrElse(IO.raiseError(new RuntimeException("No CSV sink found!")))
    }

  def upsertCsv[A](cfg: Json)(f: ResultSink.UpsertSink[IO, ColumnType.Scalar, Byte] => IO[A]): IO[A] =
    dest(cfg) {
      case Left(err) =>
        IO.raiseError(new RuntimeException(err.shows))

      case Right(dst) =>
        dst.sinks.toList
          .collectFirst { case c @ ResultSink.UpsertSink(_) => c }
          .map(s => f(s.asInstanceOf[ResultSink.UpsertSink[IO, ColumnType.Scalar, Byte]]))
          .getOrElse(IO.raiseError(new RuntimeException("No upsert CSV sink found!")))
    }

  def appendSink[A](cfg: Json)(f: ResultSink.AppendSink[IO, ColumnType.Scalar] => IO[A]): IO[A] =
    dest(cfg) {
      case Left(err) =>
        IO.raiseError(new RuntimeException(err.shows))

      case Right(dst) =>
        dst.sinks.toList
          .collectFirst { case c @ ResultSink.AppendSink(_) => c }
          .map(s => f(s.asInstanceOf[ResultSink.AppendSink[IO, ColumnType.Scalar]]))
          .getOrElse(IO.raiseError(new RuntimeException("No append CSV sink found!")))
    }

  def dest[A](cfg: Json)(f: Either[DM.InitErr, Destination[IO]] => IO[A]): IO[A] =
    DM.destination[IO](cfg, _ => _ => Stream.empty).use(f)

  trait Consumer[A]{
    def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
        table: Table,
        idColumn: Option[Column[ColumnType.Scalar]],
        writeMode: QWriteMode,
        records: Stream[IO, UpsertEvent[R]])(
        implicit
        read: Read[A],
        keys: Keys.Aux[R, K],
        values: Values.Aux[R, V],
        getTypes: Mapper.Aux[asColumnType.type, V, T],
        rrow: Mapper.Aux[renderRow.type, V, S],
        ktl: ToList[K, String],
        vtl: ToList[S, String],
        ttl: ToList[T, ColumnType.Scalar])
        : IO[(List[A], List[OffsetKey.Actual[String]])]
  }

  object Consumer {
    def upsert[A](cfg: Json, connectionUri: String): Resource[IO, Consumer[A]] = {
      val rsink: Resource[IO, ResultSink.UpsertSink[IO, ColumnType.Scalar, Byte]] =
        DM.destination[IO](cfg, _ => _ => Stream.empty) evalMap {
          case Left(err) =>
            IO.raiseError(new RuntimeException(err.shows))
          case Right(dst) =>
            val optSink = dst.sinks.toList.collectFirst { case c @ ResultSink.UpsertSink(_) => c }
            optSink match {
              case Some(s) => s.asInstanceOf[ResultSink.UpsertSink[IO, ColumnType.Scalar, Byte]].pure[IO]
              case None => IO.raiseError(new RuntimeException("No upsert sink found"))
            }
        }
      rsink map { sink => new Consumer[A] {
        def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
            table: Table,
            idColumn: Option[Column[ColumnType.Scalar]],
            writeMode: QWriteMode,
            records: Stream[IO, UpsertEvent[R]])(
            implicit
            read: Read[A],
            keys: Keys.Aux[R, K],
            values: Values.Aux[R, V],
            getTypes: Mapper.Aux[asColumnType.type, V, T],
            rrow: Mapper.Aux[renderRow.type, V, S],
            ktl: ToList[K, String],
            vtl: ToList[S, String],
            ttl: ToList[T, ColumnType.Scalar])
            : IO[(List[A], List[OffsetKey.Actual[String]])] =
        for {
          tableColumns <- columnsOf(records, renderRow, idColumn).compile.lastOrError

          colList = (idColumn.get :: tableColumns).map(k =>
            Fragment.const(hygienicIdent(k.name))).intercalate(fr",")

          dst = ResourcePath.root() / ResourceName(table)

          offsets <- toUpsertCsvSink(
            dst, sink, idColumn.get, writeMode, renderRow, records).compile.toList

          q = fr"SELECT" ++ colList ++ fr"FROM" ++ Fragment.const(hygienicIdent(table))

          rows <- runDb[IO, List[A]](q.query[A].to[List], connectionUri)

        } yield (rows, offsets)
      }}
    }

    def append[A](cfg: Json, connectionUri: String): Resource[IO, Consumer[A]] = {
      val rsink: Resource[IO, ResultSink.AppendSink[IO, ColumnType.Scalar]] =
        DM.destination[IO](cfg, _ => _ => Stream.empty) evalMap {
          case Left(err) =>
            IO.raiseError(new RuntimeException(err.shows))
          case Right(dst) =>
            val optSink = dst.sinks.toList.collectFirst { case c @ ResultSink.AppendSink(_) => c }
            optSink match {
              case Some(s) => s.asInstanceOf[ResultSink.AppendSink[IO, ColumnType.Scalar]].pure[IO]
              case None => IO.raiseError(new RuntimeException("No append sink found"))
            }
        }
      rsink map { sink => new Consumer[A] {
        def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
            table: Table,
            idColumn: Option[Column[ColumnType.Scalar]],
            writeMode: QWriteMode,
            records: Stream[IO, UpsertEvent[R]])(
            implicit
            read: Read[A],
            keys: Keys.Aux[R, K],
            values: Values.Aux[R, V],
            getTypes: Mapper.Aux[asColumnType.type, V, T],
            rrow: Mapper.Aux[renderRow.type, V, S],
            ktl: ToList[K, String],
            vtl: ToList[S, String],
            ttl: ToList[T, ColumnType.Scalar])
            : IO[(List[A], List[OffsetKey.Actual[String]])] =
        for {
          tableColumns <- columnsOf(records, renderRow, idColumn).compile.lastOrError

          colList = (idColumn.toList ++ tableColumns).map(k =>
            Fragment.const(hygienicIdent(k.name))).intercalate(fr",")

          dst = ResourcePath.root() / ResourceName(table)

          offsets <- toAppendCsvSink(
            dst, sink, idColumn, writeMode, renderRow, records).compile.toList

          q = fr"SELECT" ++ colList ++ fr"FROM" ++ Fragment.const(hygienicIdent(table))

          rows <- runDb[IO, List[A]](q.query[A].to[List], connectionUri)

        } yield (rows, offsets)
      }
    }}
  }

  object appendAndUpsert {
    def apply[A]: PartiallyApplied[A] = new PartiallyApplied[A]

    final class PartiallyApplied[A] {
      type MkOption = Column[ColumnType.Scalar] => Option[Column[ColumnType.Scalar]]
      def apply[R: AsResult](
          f: (MkOption, Consumer[A]) => IO[R])
          : SFragment = {
        "upsert" >>* Consumer.upsert[A](config(), TestConnectionUrl).use(f(Some(_), _))
        "append" >>* Consumer.append[A](config(), TestConnectionUrl).use(f(Some(_), _))
        "no-id" >>* Consumer.append[A](config(), TestConnectionUrl).use(f(x => None, _))
      }
    }
  }

  object idOnly {
    def apply[A]: PartiallyApplied[A] = new PartiallyApplied[A]

    final class PartiallyApplied[A] {
      type MkOption = Column[ColumnType.Scalar] => Option[Column[ColumnType.Scalar]]
      def apply[R: AsResult](
          f: (MkOption, Consumer[A]) => IO[R])
          : SFragment = {
        "upsert" >>* Consumer.upsert[A](config(), TestConnectionUrl).use(f(Some(_), _))
        "append" >>* Consumer.append[A](config(), TestConnectionUrl).use(f(Some(_), _))
      }
    }
  }

  def drainAndSelect[F[_]: Async: ContextShift, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      connectionUri: String,
      table: Table,
      sink: ResultSink.CreateSink[F, ColumnType.Scalar, Byte],
      records: Stream[F, R],
      schema: String = "public")(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      read: Read[V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      rrow: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      vtl: ToList[S, String],
      ttl: ToList[T, ColumnType.Scalar])
      : F[List[V]] =
    drainAndSelectAs[F, V](connectionUri, table, sink, records, schema)

  object drainAndSelectAs {
    def apply[F[_], A]: PartiallyApplied[F, A] =
      new PartiallyApplied[F, A]

    final class PartiallyApplied[F[_], A]() {
      def apply[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
          connectionUri: String,
          table: Table,
          sink: ResultSink.CreateSink[F, ColumnType.Scalar, Byte],
          records: Stream[F, R],
          schema: String = "public")(
          implicit
          async: Async[F],
          cshift: ContextShift[F],
          read: Read[A],
          keys: Keys.Aux[R, K],
          values: Values.Aux[R, V],
          getTypes: Mapper.Aux[asColumnType.type, V, T],
          rrow: Mapper.Aux[renderRow.type, V, S],
          ktl: ToList[K, String],
          vtl: ToList[S, String],
          ttl: ToList[T, ColumnType.Scalar])
          : F[List[A]] = {

        val dst = ResourcePath.root() / ResourceName(table)

        val go = records.pull.peek1 flatMap {
          case Some((r, rs)) =>
            val colList =
              r.keys.toList
                .map(k => Fragment.const(hygienicIdent(k)))
                .intercalate(fr",")

            val createSchema = fr"CREATE SCHEMA IF NOT EXISTS" ++
              Fragment.const(hygienicIdent(schema)) ++
              fr"AUTHORIZATION" ++
              Fragment.const("postgres")

            val q = fr"SELECT" ++
              colList ++
              fr"FROM" ++
              Fragment.const(hygienicIdent(schema)) ++
              fr0"." ++
              Fragment.const(hygienicIdent(table))

            val run = for {
              _ <- runDb[F, Int](createSchema.update.run, connectionUri)
              _ <- toCsvSink(dst, sink, renderRow, rs).compile.drain
              rows <- runDb[F, List[A]](q.query[A].to[List], connectionUri)
            } yield rows

            Pull.eval(run).flatMap(Pull.output1)

          case None =>
            Pull.done
        }

        go.stream.compile.last.map(_ getOrElse Nil)
      }
    }
  }

  def loadAndRetrieve[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      record: R,
      records: R*)(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      read: Read[V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      rrow: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      vtl: ToList[S, String],
      ttl: ToList[T, ColumnType.Scalar])
      : IO[List[V]] =
    randomAlphaNum[IO](6) flatMap { tableSuffix =>
      val table = s"pgdest_test_${tableSuffix}"
      val cfg = config(url = TestConnectionUrl)
      val rs = record +: records

      csv(cfg)(drainAndSelect(TestConnectionUrl, table, _, Stream.emits(rs)))
    }

  def mustRoundtrip[R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      record: R,
      records: R*)(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      read: Read[V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      rrow: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      vtl: ToList[S, String],
      ttl: ToList[T, ColumnType.Scalar])
      : IO[MatchResult[List[V]]] =
    loadAndRetrieve(record, records: _*)
      .map(_ must containTheSameElementsAs(record +: records))
}
