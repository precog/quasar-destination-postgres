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

import scala._

import fs2.Chunk
import fs2.Stream
import fs2.Pipe
import fs2.Pull

import quasar.api.push.OffsetKey
import quasar.connector.AppendEvent
import quasar.connector.DataEvent
import quasar.connector.destination.ResultSink
import quasar.connector.render.RenderConfig

import skolems.∀

object RechunkingSink {

  /**
   * Returns a modified result sink that rechunks the input stream, combining
   * adjacent byte chunks until their size is >= `chunkSize`. This is to avoid splitting
   * input chunks.
   *
   * Resulting chunks may be larger than `chunkSize` by the size of one input chunk.
   */
  def apply[F[_], T](chunkSize: Int): ResultSink[F, T] => ResultSink[F, T] = {
    case ResultSink.CreateSink(f) =>
      ResultSink.create((p, cs) => rechunkCreateBytes(chunkSize)(f(p, cs)))

    case ResultSink.UpsertSink(f) =>
      ResultSink.upsert(f andThen rechunkUpsertBytes(chunkSize))

    case ResultSink.AppendSink(f) =>
      ResultSink.AppendSink[F, T] { args =>
        val res = f(args)

        new ResultSink.AppendSink.Result[F] {
          type A = res.A
          val renderConfig = res.renderConfig
          val pipe = rechunkAppendBytes(chunkSize)((res.renderConfig, res.pipe))
        }
      }
  }

  def rechunkCreateBytes[F[_], A](
      chunkSize: Int)(
      sink: (RenderConfig[A], Pipe[F, A, Unit]))
      : (RenderConfig[A], Pipe[F, A, Unit]) =
    sink match {
      case (csv: RenderConfig.Csv, pipe) =>
        (csv, pipe.compose(rechunkByteStream[F](chunkSize)))

      case (js: RenderConfig.Json, pipe) =>
        (js, pipe.compose(rechunkByteStream[F](chunkSize)))

      case other => other
    }

  def rechunkUpsertBytes[F[_], A](
      chunkSize: Int)(
      sink: (RenderConfig[A], ∀[λ[α => Pipe[F, DataEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]))
      : (RenderConfig[A], ∀[λ[α => Pipe[F, DataEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]) =
    sink match {
      case (csv: RenderConfig.Csv, pipe) =>
        val rechunked =
          new ∀[λ[α => Pipe[F, DataEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]] {
            def apply[X]: Pipe[F, DataEvent[A, OffsetKey.Actual[X]], OffsetKey.Actual[X]] = {
              type O = OffsetKey.Actual[X]
              _.through(rechunkEventStream[F, O, DataEvent[Byte, O]](chunkSize)).through(pipe[X])
            }
          }

        (csv, rechunked)

      case (js: RenderConfig.Json, pipe) =>
        val rechunked =
          new ∀[λ[α => Pipe[F, DataEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]] {
            def apply[X]: Pipe[F, DataEvent[A, OffsetKey.Actual[X]], OffsetKey.Actual[X]] = {
              type O = OffsetKey.Actual[X]
              _.through(rechunkEventStream[F, O, DataEvent[Byte, O]](chunkSize)).through(pipe[X])
            }
          }

        (js, rechunked)

      case other => other
    }

  def rechunkAppendBytes[F[_], A](
      chunkSize: Int)(
        sink: (RenderConfig[A], ∀[λ[α => Pipe[F, AppendEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]))
      : ∀[λ[α => Pipe[F, AppendEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]] =
    sink match {
      case (csv: RenderConfig.Csv, pipe) =>
        new ∀[λ[α => Pipe[F, AppendEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]] {
          def apply[X]: Pipe[F, AppendEvent[A, OffsetKey.Actual[X]], OffsetKey.Actual[X]] = {
            type O = OffsetKey.Actual[X]
            _.through(rechunkEventStream[F, O, AppendEvent[Byte, O]](chunkSize)).through(pipe[X])
          }
        }

      case (js: RenderConfig.Json, pipe) =>
        new ∀[λ[α => Pipe[F, AppendEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]] {
          def apply[X]: Pipe[F, AppendEvent[A, OffsetKey.Actual[X]], OffsetKey.Actual[X]] = {
            type O = OffsetKey.Actual[X]
            _.through(rechunkEventStream[F, O, AppendEvent[Byte, O]](chunkSize)).through(pipe[X])
          }
        }

      case (_, other) => other
    }

  def rechunkByteStream[F[_]](chunkSize: Int): Pipe[F, Byte, Byte] =
    _.chunks
      .map(DataEvent.Create(_))
      .through(rechunkEventStream[F, Nothing, DataEvent.Create[Byte]](chunkSize))
      .flatMap(c => Stream.chunk(c.records))

  def rechunkEventStream[F[_], A, E >: DataEvent.Create[Byte] <: DataEvent[Byte, A]](chunkSize: Int): Pipe[F, E, E] = {
    def go(acc: Option[Chunk[Byte]], s: Stream[F, E]): Pull[F, E, Unit] =
      s.pull.uncons1.flatMap {
        case Some((e @ DataEvent.Create(c), tl)) =>
          acc match {
            case Some(c0) =>
              val c1 = Chunk.concatBytes(List(c0, c))
              if (c1.size >= chunkSize)
                Pull.output1(DataEvent.Create(c1)) >> go(None, tl)
              else
                go(Some(c1), tl)

            case None if c.size >= chunkSize =>
              Pull.output1(e) >> go(None, tl)

            case None =>
              go(Some(c), tl)
          }

        case Some((other, tl)) =>
          val emit =
            acc.fold[Pull[F, E, Unit]](
              Pull.output1(other))(
              c => Pull.output1(DataEvent.Create(c)) >> Pull.output1(other))

          emit >> go(None, tl)

        case None =>
          acc.fold[Pull[F, E, Unit]](Pull.done)(c => Pull.output1(DataEvent.Create(c)))
      }

    go(None, _).stream
  }
}
