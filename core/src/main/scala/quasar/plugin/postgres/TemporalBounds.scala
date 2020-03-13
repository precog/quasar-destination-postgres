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

import java.time._

object TemporalBounds {
  val MinLocalTime: LocalTime = LocalTime.MIN
  val MaxLocalTime: LocalTime = LocalTime.MAX.minusNanos(999)

  val MinLocalDate: LocalDate = LocalDate.of(-4713, 11, 24)
  val MaxLocalDate: LocalDate = LocalDate.of(5874897, 12, 31)

  val MinLocalDateTime: LocalDateTime = MinLocalDate.atTime(MinLocalTime)

  // PG uses a fixed number of bits in the representation, so the larger the year
  // the less available for fraction-of-second precision.
  val MaxLocalDateTime: LocalDateTime =
    LocalDate.of(294276, 12, 31)
      .atTime(LocalTime.MAX.minusNanos(99999999))

  val MinOffset: ZoneOffset = ZoneOffset.ofHoursMinutes(14, 59)
  val MaxOffset: ZoneOffset = ZoneOffset.ofHoursMinutes(-14, -59)

  val MinOffsetTime: OffsetTime = MinLocalTime.atOffset(MinOffset)
  val MaxOffsetTime: OffsetTime = MaxLocalTime.atOffset(MaxOffset)

  val MinOffsetDateTime: OffsetDateTime =
    MinLocalDateTime
      .plusHours(14)
      .plusMinutes(59)
      .atOffset(MinOffset)

  val MaxOffsetDateTime: OffsetDateTime =
    MaxLocalDateTime
      .minusHours(14)
      .minusMinutes(59)
      .atOffset(MaxOffset)
}
