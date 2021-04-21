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

import argonaut._, Argonaut._

import cats.implicits._

import java.net.URI

import quasar.lib.jdbc.destination.WriteMode

import scala.concurrent.duration._
import scala.util.control.NonFatal

final case class Config(
    connectionUri: URI,
    schema: Option[String],
    writeMode: Option[WriteMode],
    retryTransactionTimeoutMs: Option[Int],
    maxTransactionReattempts: Option[Int]) {

  def retryTransactionTimeout: FiniteDuration =
    retryTransactionTimeoutMs.map(_.milliseconds) getOrElse Config.DefaultTimeout

  def maxRetries: Int =
    maxTransactionReattempts getOrElse Config.DefaultMaxReattempts

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def sanitized: Config = {
    val sanitizedUserInfo =
      Option(connectionUri.getUserInfo) map { ui =>
        val colon = ui.indexOf(':')

        if (colon === -1)
          ui
        else
          ui.substring(0, colon) + ":<redacted>"
      }

    val sanitizedQuery =
      Option(connectionUri.getQuery) map { q =>
        val pairs = q.split('&').toList map { kv =>
          if (kv.toLowerCase.startsWith("password"))
            s"password=$Redacted"
          else if (kv.toLowerCase.startsWith("sslpassword"))
            s"sslpassword=$Redacted"
          else
            kv
        }

        pairs.intercalate("&")
      }

    copy(connectionUri = new URI(
      connectionUri.getScheme,
      sanitizedUserInfo.orNull,
      connectionUri.getHost,
      connectionUri.getPort,
      connectionUri.getPath,
      sanitizedQuery.orNull,
      connectionUri.getFragment))
  }
}

object Config {
  def DefaultTimeout: FiniteDuration = 60.seconds

  def DefaultMaxReattempts: Int = 10

  implicit val codecJson: CodecJson[Config] = {
    implicit val uriDecodeJson: DecodeJson[URI] =
      DecodeJson(c => c.as[String] flatMap { s =>
        try {
          DecodeResult.ok(new URI(s))
        } catch {
          case NonFatal(t) => DecodeResult.fail("URI", c.history)
        }
      })

    implicit val uriEncodeJson: EncodeJson[URI] =
      EncodeJson.of[String].contramap(_.toString)

    casecodec5(Config.apply, Config.unapply)(
      "connectionUri",
      "schema",
      "writeMode",
      "retryTransactionTimeoutMs",
      "maxTransactionReattempts")
  }
}
