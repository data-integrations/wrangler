/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.steps.transformation.functions;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Period;
import org.joda.time.Seconds;

import java.util.Date;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column <expression>
 */
public class DateFunctions {
  /**
   * Converts a date to long -- unix timestamp in milli-seconds.
   *
   * @param date to be converted to unix timestamp.
   * @return unixtimestamp of the date.
   */
  public static long UNIXTIMESTAMP_MILLIS(Date date) {
    return date.getTime();
  }

  /**
   * Converts a date to long -- unix timestamp in seconds.
   *
   * @param date to be converted to unix timestamp.
   * @return unixtimestamp of the date.
   */
  public static long UNIXTIMESTAMP_SECONDS(Date date) {
    return date.getTime() / 1000;
  }

  /**
   * Converts a {@link Date} to Month in year.
   * <p>
   *   January is 1, February is 2, and so on.
   * </p>
   * @param date to extract month.
   * @return month.
   */
  public static int MONTH(Date date) {
    DateTime dt = new DateTime(date);
    DateTime.Property pMoY = dt.monthOfYear();
    return pMoY.get();
  }

  /**
    * Extracts a short month description from Date.
   *
   * @param date to extract short month description.
   * @return short month description.
   */
  public static String MONTH_SHORT(Date date) {
    DateTime dt = new DateTime(date);
    DateTime.Property pMoY = dt.monthOfYear();
    return pMoY.getAsShortText();
  }

  /**
   * Extracts a long month description from Date.
   *
   * @param date to extract long month description.
   * @return long month description.
   */
  public static String MONTH_LONG(Date date) {
    DateTime dt = new DateTime(date);
    DateTime.Property pMoY = dt.monthOfYear();
    return pMoY.getAsText();
  }

  /**
   * Extracts only year from Date.
   *
   * @param date to extract year from.
   * @return year as integer.
   */
  public static int YEAR(Date date) {
    DateTime dt = new DateTime(date);
    return dt.getYear();
  }

  /**
   * Extracts day of the week from the date.
   *
   * @param date to extract date of the week.
   * @return day of the week.
   */
  public static int DAY_OF_WEEK(Date date) {
    DateTime dt = new DateTime(date);
    DateTime.Property value = dt.dayOfWeek();
    return value.get();
  }

  /**
   * Extracts day of the week from the date.
   *
   * @param date to extract date of the week.
   * @return day of the week.
   */
  public static String DAY_OF_WEEK_SHORT(Date date) {
    DateTime dt = new DateTime(date);
    DateTime.Property value = dt.dayOfWeek();
    return value.getAsShortText();
  }

  /**
   * Extracts day of the week from the date.
   *
   * @param date to extract date of the week.
   * @return day of the week.
   */
  public static String DAY_OF_WEEK_LONG(Date date) {
    DateTime dt = new DateTime(date);
    DateTime.Property value = dt.dayOfWeek();
    return value.getAsText();
  }

  /**
   * Extracts Day of the year from the date.
   *
   * @param date to extract date of the year.
   * @return date of the year.
   */
  public static int DAY_OF_YEAR(Date date) {
    DateTime dt = new DateTime(date);
    DateTime.Property value = dt.dayOfYear();
    return value.get();
  }

  /**
   * Extracts Era from the date.
   *
   * @param date to extract era.
   * @return era.
   */
  public static int ERA(Date date) {
    DateTime dt = new DateTime(date);
    DateTime.Property value = dt.era();
    return value.get();
  }

  /**
   * Extracts Era from the date as short text.
   *
   * @param date to extract era.
   * @return era.
   */
  public static String ERA_SHORT(Date date) {
    DateTime dt = new DateTime(date);
    DateTime.Property value = dt.era();
    return value.getAsShortText();
  }

  /**
   * Extracts Era from the date as long text.
   *
   * @param date to extract era.
   * @return era.
   */
  public static String ERA_LONG(Date date) {
    DateTime dt = new DateTime(date);
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
  public static int DAYS_BETWEEN(Date date1, Date date2) {
    DateTime dt1 = new DateTime(date1);
    DateTime dt2 = new DateTime(date2);
    return Days.daysBetween(dt1, dt2).getDays();
  }

  /**
   * Return number of dates between now and date days.
   *
   * @param date Now - this date.
   * @return Number of days.
   */
  public static int DAYS_BETWEEN_NOW(Date date) {
    DateTime dt1 = new DateTime();
    DateTime dt2 = new DateTime(date);
    return Days.daysBetween(dt1, dt2).getDays();
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

}
