/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.functions;

import io.cdap.wrangler.dq.TypeInference;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.Seconds;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import static java.time.temporal.ChronoField.ERA;
import static java.time.temporal.ChronoUnit.DAYS;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column <expression>
 */
@Deprecated
public final class Dates {
  /**
   * Converts a date to long -- unix timestamp in milli-seconds.
   *
   * @param date to be converted to unix timestamp.
   * @return unixtimestamp of the date.
   */
  public static long UNIXTIMESTAMP_MILLIS(ZonedDateTime date) {
    validate(date, "UNIXTIMESTAMP_MILLIS");
    return date.toInstant().toEpochMilli();
  }

  /**
   * Converts a date to long -- unix timestamp in seconds.
   *
   * @param date to be converted to unix timestamp.
   * @return unixtimestamp of the date.
   */
  public static long UNIXTIMESTAMP_SECONDS(ZonedDateTime date) {
    validate(date, "UNIXTIMESTAMP_SECONDS");
    return date.toEpochSecond();
  }

  /**
   * Converts a {@link ZonedDateTime} to Month in year.
   * <p>
   *   January is 1, February is 2, and so on.
   * </p>
   * @param date to extract month.
   * @return month.
   */
  public static int MONTH(ZonedDateTime date) {
    validate(date, "MONTH");
    return date.getMonthValue();
  }

  /**
    * Extracts a short month description from Date.
   *
   * @param date to extract short month description.
   * @return short month description.
   */
  public static String MONTH_SHORT(ZonedDateTime date) {
    validate(date, "MONTH_SHORT");
    DateTime dt = getDateTime(date);
    DateTime.Property pMoY = dt.monthOfYear();
    return pMoY.getAsShortText();
  }

  /**
   * Extracts a long month description from Date.
   *
   * @param date to extract long month description.
   * @return long month description.
   */
  public static String MONTH_LONG(ZonedDateTime date) {
    validate(date, "MONTH_LONG");
    DateTime dt = getDateTime(date);
    DateTime.Property pMoY = dt.monthOfYear();
    return pMoY.getAsText();
  }

  /**
   * Extracts only year from Date.
   *
   * @param date to extract year from.
   * @return year as integer.
   */
  public static int YEAR(ZonedDateTime date) {
    validate(date, "YEAR");
    return date.getYear();
  }

  /**
   * Extracts day of the week from the date.
   *
   * @param date to extract date of the week.
   * @return day of the week.
   */
  public static int DAY_OF_WEEK(ZonedDateTime date) {
    validate(date, "DAY_OF_WEEK");
    return date.getDayOfWeek().getValue();
  }

  /**
   * Extracts day of the week from the date.
   *
   * @param date to extract date of the week.
   * @return day of the week.
   */
  public static String DAY_OF_WEEK_SHORT(ZonedDateTime date) {
    validate(date, "DAY_OF_WEEK_SHORT");
    DateTime dt = getDateTime(date);
    DateTime.Property value = dt.dayOfWeek();
    return value.getAsShortText();
  }

  /**
   * Extracts day of the week from the date.
   *
   * @param date to extract date of the week.
   * @return day of the week.
   */
  public static String DAY_OF_WEEK_LONG(ZonedDateTime date) {
    validate(date, "DAY_OF_WEEK_LONG");
    DateTime dt = getDateTime(date);
    DateTime.Property value = dt.dayOfWeek();
    return value.getAsText();
  }

  /**
   * Extracts Day of the year from the date.
   *
   * @param date to extract date of the year.
   * @return date of the year.
   */
  public static int DAY_OF_YEAR(ZonedDateTime date) {
    validate(date, "DAY_OF_YEAR");
    return date.getDayOfYear();
  }

  /**
   * Extracts Era from the date.
   *
   * @param date to extract era.
   * @return era.
   */
  public static int ERA(ZonedDateTime date) {
    validate(date, "ERA");
    return date.get(ERA);
  }

  /**
   * Extracts Era from the date as short text.
   *
   * @param date to extract era.
   * @return era.
   */
  public static String ERA_SHORT(ZonedDateTime date) {
    validate(date, "ERA_SHORT");
    DateTime dt = getDateTime(date);
    DateTime.Property value = dt.era();
    return value.getAsShortText();
  }

  /**
   * Extracts Era from the date as long text.
   *
   * @param date to extract era.
   * @return era.
   */
  public static String ERA_LONG(ZonedDateTime date) {
    validate(date, "ERA_LONG");
    DateTime dt = getDateTime(date);
    DateTime.Property value = dt.era();
    return value.getAsText();
  }

  /**
   * Return number of dates between two days.
   *
   * @param date1 First date.
   * @param date2 Second date.
   * @return Number of days.
   */
  public static int DAYS_BETWEEN(ZonedDateTime date1, ZonedDateTime date2) {
    validate(date1, "ERA_LONG");
    validate(date2, "ERA_LONG");
    return (int) DAYS.between(date1, date2);
  }

  /**
   * Return number of dates between now and date days.
   *
   * @param date Now - this date.
   * @return Number of days.
   */
  public static int DAYS_BETWEEN_NOW(ZonedDateTime date) {
    validate(date, "DAYS_BETWEEN_NOW");
    ZonedDateTime now = ZonedDateTime.now(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    // returns number of days between date and now, date being lower.
    return (int) DAYS.between(date, now);
  }

  /**
   * Converts seconds to days.
   *
   * @param seconds to be converted.
   * @return days equivalent of seconds
   */
  public static int SECONDS_TO_DAYS(int seconds) {
    Period period = new Period(Seconds.seconds(seconds));
    return period.toStandardDays().getDays();
  }

  /**
   * Converts seconds to hours.
   *
   * @param seconds to be converted.
   * @return hours equivalent of seconds
   */
  public static int SECONDS_TO_HOURS(int seconds) {
    Period period = new Period(Seconds.seconds(seconds));
    return period.toStandardHours().getHours();
  }

  /**
   * Converts seconds to mins.
   *
   * @param seconds to be converted.
   * @return mins equivalent of seconds
   */
  public static int SECONDS_TO_MINUTES(int seconds) {
    Period period = new Period(Seconds.seconds(seconds));
    return period.toStandardMinutes().getMinutes();
  }

  /**
   * Converts seconds to weeks.
   *
   * @param seconds to be converted.
   * @return weeks equivalent of seconds
   */
  public static int SECONDS_TO_WEEKS(int seconds) {
    Period period = new Period(Seconds.seconds(seconds));
    return period.toStandardWeeks().getWeeks();
  }

  /**
   * Checks if a column is a date column or not.
   *
   * @param value representing a date.
   * @return true if it's date, false if not.
   */
  public static boolean isDate(String value) {
    return TypeInference.isDate(value);
  }

  /**
   * Checks if the value passed is a date time.
   *
   * @param value representing date time.
   * @return true if it's datetime, false if not.
   */
  public static boolean isTime(String value) {
    return TypeInference.isTime(value);
  }

  private static DateTime getDateTime(ZonedDateTime zonedDateTime) {
    return new DateTime(
      zonedDateTime.toInstant().toEpochMilli(),
      DateTimeZone.forTimeZone(TimeZone.getTimeZone(zonedDateTime.getZone())));
  }

  private static void validate(ZonedDateTime date, String method) {
    if (date == null) {
      throw new IllegalArgumentException(String.format("Date can not be null for %s", method));
    }
  }
}
