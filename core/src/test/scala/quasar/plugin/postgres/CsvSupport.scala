/*
 * Copyright 2014–2020 SlamData Inc.
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

import cats.ApplicativeError
import cats.data.NonEmptyList

import com.github.tototoshi.csv._

import fs2._

import java.io.ByteArrayOutputStream
import java.time._

import qdata.time._
import quasar.api.{Column, ColumnType}
import quasar.api.resource.ResourcePath
import quasar.connector.destination.ResultSink
import quasar.connector.render.RenderConfig

import scala.Float
import scala.collection.immutable.Seq

import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}
import shapeless.ops.record.{Keys, Values}
import shapeless.record._

trait CsvSupport {

  // Must remain in sync with Quasar's CSV output format to be representative.
  val QuasarCSVFormat: CSVFormat =
    new CSVFormat {
      val delimiter = ','
      val quoteChar = '"'
      val escapeChar = '"'
      val lineTerminator = "\r\n"
      val quoting = QUOTE_MINIMAL
      val treatEmptyLineAsNil = false
    }

  // TODO: handle includeHeader == true
  def toCsvSink[F[_]: ApplicativeError[?[_], Throwable], P <: Poly1, R <: HList, K <: HList, V <: HList, T <: HList, S <: HList](
      dst: ResourcePath,
      sink: ResultSink.CreateSink[F, ColumnType.Scalar],
      renderRow: P,
      records: Stream[F, R])(
      implicit
      keys: Keys.Aux[R, K],
      values: Values.Aux[R, V],
      getTypes: Mapper.Aux[asColumnType.type, V, T],
      renderValues: Mapper.Aux[renderRow.type, V, S],
      ktl: ToList[K, String],
      stl: ToList[S, String],
      ttl: ToList[T, ColumnType.Scalar])
      : Stream[F, Unit] = {

    val go = records.pull.peek1 flatMap {
      case Some((r, rs)) =>
        val rkeys = r.keys.toList
        val rtypes = r.values.map(asColumnType).toList
        val columns = rkeys.zip(rtypes).map((Column[ColumnType.Scalar] _).tupled)
        val encoded = rs.through(encodeCsvRecords[F, renderRow.type, R, V, S](renderRow))

        sink.consume(dst, NonEmptyList.fromListUnsafe(columns), encoded).pull.echo

      case None => Pull.done
    }

    go.stream
  }

  def encodeCsvRecords[F[_]: ApplicativeError[?[_], Throwable], P <: Poly1, R <: HList, V <: HList, S <: HList](
      renderRow: P)(
      implicit
      values: Values.Aux[R, V],
      render: Mapper.Aux[renderRow.type, V, S],
      ltl: ToList[S, String])
      : Pipe[F, R, Byte] =
    _.map(_.values.map(renderRow).toList).through(encodeCsvRows[F])

  class renderForCsv(cfg: RenderConfig.Csv) extends Poly1 {
    implicit val boolCase = at[Boolean](_.toString)

    implicit val localTimeCase = at[LocalTime](_.format(cfg.localTimeFormat))
    implicit val offsetTimeCase = at[OffsetTime](_.format(cfg.offsetTimeFormat))

    implicit val localDateCase = at[LocalDate](_.format(cfg.localDateFormat))
    implicit val offsetDateCase = at[OffsetDate](cfg.offsetDateFormat.format)

    implicit val localDateTimeCase = at[LocalDateTime](_.format(cfg.localDateTimeFormat))
    implicit val offsetDateTimeCase = at[OffsetDateTime](_.format(cfg.offsetDateTimeFormat))

    implicit val dateTimeIntervalCase = at[DateTimeInterval](_.toString)

    implicit val shortCase = at[Short](_.toString)
    implicit val intCase = at[Int](_.toString)
    implicit val longCase = at[Long](_.toString)
    implicit val floatCase = at[Float](_.toString)
    implicit val doubleCase = at[Double](_.toString)
    implicit val bigDecCase = at[BigDecimal](_.toString)

    implicit val charCase = at[Char](_.toString)
    implicit val stringCase = at[String](s => s)
  }

  object asColumnType extends Poly1 {
    implicit val boolCase = at[Boolean](_ => ColumnType.Boolean)

    implicit val localTimeCase = at[LocalTime](_ => ColumnType.LocalTime)
    implicit val offsetTimeCase = at[OffsetTime](_ => ColumnType.OffsetTime)

    implicit val localDateCase = at[LocalDate](_ => ColumnType.LocalDate)
    implicit val offsetDateCase = at[OffsetDate](_ => ColumnType.OffsetDate)

    implicit val localDateTimeCase = at[LocalDateTime](_ => ColumnType.LocalDateTime)
    implicit val offsetDateTimeCase = at[OffsetDateTime](_ => ColumnType.OffsetDateTime)

    implicit val dateTimeIntervalCase = at[DateTimeInterval](_ => ColumnType.Interval)

    implicit val shortCase = at[Short](_ => ColumnType.Number)
    implicit val intCase = at[Int](_ => ColumnType.Number)
    implicit val longCase = at[Long](_ => ColumnType.Number)
    implicit val floatCase = at[Float](_ => ColumnType.Number)
    implicit val doubleCase = at[Double](_ => ColumnType.Number)
    implicit val bigDecCase = at[BigDecimal](_ => ColumnType.Number)

    implicit val charCase = at[Char](_ => ColumnType.String)
    implicit val stringCase = at[String](_ => ColumnType.String)
  }

  ////

  private def encodeCsvRows[F[_]](implicit F: ApplicativeError[F, Throwable]): Pipe[F, Seq[String], Byte] =
    in => Stream.suspend {
      val os = new ByteArrayOutputStream
      val csvWriter = CSVWriter.open(os, "UTF-8")(QuasarCSVFormat)

      def drainBytes: Stream[F, Byte] = {
        val bs = os.toByteArray
        os.reset()
        Stream.chunk(Chunk.bytes(bs))
      }

      val process: Stream[F, Byte] =
        for {
          row <- in

          _ <- Stream.eval(F catchNonFatal {
            csvWriter.writeRow(row)
            csvWriter.flush()
          })

          b <- drainBytes
        } yield b

      val finish: Stream[F, Byte] =
        Stream.suspend {
          csvWriter.close()
          drainBytes
        }

      process ++ finish
    }
}

object CsvSupport extends CsvSupport
