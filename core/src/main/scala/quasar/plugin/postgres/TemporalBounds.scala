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

package quasar.plugin.postgres

import java.time._

object TemporalBounds {
  val MinLocalTime: LocalTime = LocalTime.MIN
  val MaxLocalTime: LocalTime = LocalTime.MAX.minusNanos(999)

  val MinLocalDate: LocalDate = LocalDate.of(-4714, 11, 24)
  val MaxLocalDate: LocalDate = LocalDate.of(5874897, 12, 31)

  val MinLocalDateTime: LocalDateTime = MinLocalDate.atTime(MinLocalTime)
  val MaxLocalDateTime: LocalDateTime = LocalDate.of(294276, 12, 31).atTime(MaxLocalTime)

  val MinOffset: ZoneOffset = ZoneOffset.ofHoursMinutes(14, 59)
  val MaxOffset: ZoneOffset = ZoneOffset.ofHoursMinutes(-14, 59)

  val MinOffsetTime: OffsetTime = MinLocalTime.atOffset(MinOffset)
  val MaxOffsetTime: OffsetTime = MaxLocalTime.atOffset(MaxOffset)

  val MinOffsetDateTime: OffsetDateTime = MinLocalDateTime.atOffset(MinOffset)
  val MaxOffsetDateTime: OffsetDateTime = MaxLocalDateTime.atOffset(MaxOffset)
}
