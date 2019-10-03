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

package quasar.plugin

import slamdata.Predef._

import cats.effect.Sync

import java.net.URI
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, SignStyle}
import java.time.temporal.ChronoField

import quasar.api.push.RenderConfig
import quasar.api.resource._

import scala.util.Random

package object postgres {

  type Ident = String
  type Table = Ident

  val Redacted: String = "--REDACTED--"

  val PostgresCsvConfig: RenderConfig.Csv = {
    val eraPattern = " G"

    val time =
      DateTimeFormatter.ofPattern("HH:mm:ss.S")

    // mutable builder
    def unsignedDate =
      (new DateTimeFormatterBuilder())
        .appendValue(ChronoField.YEAR_OF_ERA, 4, 19, SignStyle.NEVER)
        .appendPattern("-MM-dd")

    RenderConfig.Csv(
      includeHeader = false,
      includeBom = false,

      offsetDateTimeFormat =
        unsignedDate
          .appendLiteral(' ')
          .append(time)
          .appendPattern("Z" + eraPattern)
          .toFormatter,

      localDateTimeFormat =
        unsignedDate
          .appendLiteral(' ')
          .append(time)
          .appendPattern(eraPattern)
          .toFormatter,

      localDateFormat =
        unsignedDate
          .appendPattern(eraPattern)
          .toFormatter)
  }

  /** Returns a quoted and escaped version of `ident`. */
  def hygienicIdent(ident: Ident): Ident =
    s""""${ident.replace("\"", "\"\"")}""""

  /** Returns the JDBC connection string corresponding to the given postgres URI. */
  def jdbcUri(pgUri: URI): String =
    s"jdbc:${pgUri}"

  /** Returns a random alphanumeric string of the specified length. */
  def randomAlphaNum[F[_]: Sync](size: Int): F[String] =
    Sync[F].delay(Random.alphanumeric.take(size).mkString)

  /** Attempts to extract a table name from the given path. */
  def tableFromPath(p: ResourcePath): Option[Table] =
    Some(p) collect {
      case table /: ResourcePath.Root => table
    }
}
