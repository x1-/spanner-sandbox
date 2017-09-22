package com.inkenkun.x1.spanner

import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import com.google.cloud.Date
import io.circe.{ObjectEncoder, Printer}
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.syntax._

package object sandbox {
  implicit val customConfig: Configuration          = Configuration.default.withSnakeCaseKeys
  implicit val encoder:      ObjectEncoder[Example] = deriveEncoder

  val BqDateTimeFormat     : DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss VV")

  case class Example (
    unique: String,
    time  : String,
    sz    : String,
    ps    : String,
    rad   : Long,
    num1  : Long,
    num2  : Long
  ) {
    def toJson: String =
      this.asJson.pretty(Printer.noSpaces.copy(dropNullKeys = false))
  }

  implicit class DateToZonedDateTime(val self: Date) extends AnyVal {
    def toZoned: ZonedDateTime = ZonedDateTime.of(
      self.getYear, self.getMonth, self.getDayOfMonth, 0, 0, 0, 0, ZoneId.of("UTC")
    )
  }
  implicit class ZonedDateTimeToDate(val self: ZonedDateTime) extends AnyVal {
    def toDateAtUTC: Date = {
      val zd = if (self.getZone == ZoneId.of("UTC")) self
               else self.withZoneSameInstant(ZoneId.of("UTC"))
      Date.fromYearMonthDay(zd.getYear, zd.getMonthValue, zd.getDayOfMonth)
    }
  }
}
