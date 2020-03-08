/*
 *  Copyright © 2020 Cask Data, Inc.
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

import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Days;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column datetime:CurrentDate()
 * set-column :column exp:{ datetime:CurrrentDate()  }
 */
public final class DateAndTime {
  private static String DATE_FORMAT = "yyyy-MM-dd";

  private static DateTime getFormattedDateTime(String baseline) {
    return DateTime.parse(baseline, DateTimeFormat.forPattern(DATE_FORMAT));
  }
  /**
   * @return returns current date with time.
   */
  public static DateTime CurrentDate() {
    return new DateTime();
  }

  /**
   * @return return current localtime.
   */
  public static LocalTime CurrentTime() {
    DateTime dt = new DateTime();
    return dt.toLocalTime();
  }

  /**
   * @return return current time in milliseconds.
   */
  public static long CurrentTimeMS() {
    DateTime dt = new DateTime();
    return dt.getMillis();
  }

  /**
   * @return return current data & time.
   */
  public static DateTime CurrentTimestamp() {
    return new DateTime();
  }

  /**
   * @return return current data & time in milliseconds.
   */
  public static long CurrentTimestampMS() {
    return new DateTime().getMillis();
  }

  /**
   * Returns a date object by adding an integer to a current date.
   *
   * @param days to be added to current date.
   * @return returns a {@link DateTime} by adding days to current date.
   */
  public static DateTime DateFromDaysSince(int days) {
    DateTime baseline = new DateTime();
    return baseline.plusDays(days);
  }

  /**
   * Returns a date object by adding an integer to a baseline date.
   * The integer can be negative to return a date that is earlier than the baseline date.
   *
   * @param days to be added to the baseline date.
   * @param baseline date to which 'days' are added.
   * @return days added positive or negative to the baseline date.
   */
  public static DateTime DateFromDaysSince(int days, String baseline) {
    DateTime baselineDate = getFormattedDateTime(baseline);
    // DateTime.parse(baseline, DateTimeFormat.forPattern(DATE_FORMAT));
    return baselineDate.plusDays(days);
  }

  /**
   * Creates a {@link DateTime} from the components year, month, and day.
   *
   * @param year to used for date.
   * @param month to be used for date.
   * @param day to be used for date
   * @return a instance of {@link DateTime}
   */
  public static DateTime DateFromComponents(int year, int month, int day) {
    return new DateTime(year, month, day, 0, 0);
  }

  /**
   * Returns a date from the given Julian day number.
   *
   * @param julianDay Julian day number
   * @return a instance of {@link DateTime} that represents the Julian day.
   */
  public static DateTime DateFromJulianDay(int julianDay) {
    long l = DateTimeUtils.fromJulianDay(julianDay);
    return new DateTime(l);
  }

  /**
   * Returns the given date, with offsets applied from the given year offset, month offset, and day of month offset.
   * offsets are given as three separate values. The offset values can each be positive, zero, or negative.
   *
   * @param baseline baseline date.
   * @param yearOffset year offset to be applied.
   * @param monthOffset month offset to be applied.
   * @param daysOffset days offset to be applied.
   * @return Modified date with offsets applied.
   */
  public static DateTime DateOffsetByComponents(DateTime baseline, int yearOffset, int monthOffset, int daysOffset) {
    return baseline.plusYears(yearOffset).plusMonths(monthOffset).plusDays(daysOffset);
  }

  /**
   * Returns the given date, with offsets applied from the given year offset, month offset, and day of month offset.
   * offsets are given as three separate values. The offset values can each be positive, zero, or negative.
   *
   * @param baseline baseline date as string.
   * @param yearOffset year offset to be applied.
   * @param monthOffset month offset to be applied.
   * @param daysOffset days offset to be applied.
   * @return Modified date with offsets applied.
   */
  public static DateTime DateOffsetByComponents(String baseline, int yearOffset, int monthOffset, int daysOffset) {
    DateTime baselineDate = getFormattedDateTime(baseline);
    return baselineDate.plusYears(yearOffset).plusMonths(monthOffset).plusDays(daysOffset);
  }

  /**
   * Returns the number of days from the source date to the given date.
   * @param source date of {@link DateTime} type.
   * @param destination date of {@link DateTime} type.
   * @return Number of days between the dates.
   */
  public static int DaysSinceFromDate(DateTime source, DateTime destination) {
    return Days.daysBetween(source, destination).getDays();
  }

  /**
   * Returns the number of days from the source date to the given date.
   *
   * @param source date of {@link DateTime} type.
   * @param destination date of {@link String} type.
   * @return Number of days between the dates.
   */
  public static int DaysSinceFromDate(DateTime source, String destination) {
    return Days.daysBetween(source, getFormattedDateTime(destination)).getDays();
  }

  /**
   * Returns the number of days from the source date to the given date.
   *
   * @param source date of {@link String} type.
   * @param destination date of {@link DateTime} type.
   * @return Number of days between the dates.
   */
  public static int DaysSinceFromDate(String source, DateTime destination) {
    return Days.daysBetween(getFormattedDateTime(source), destination).getDays();
  }

  /**
   * Returns the number of days from the source date to the given date.
   *
   * @param source date of {@link String} type.
   * @param destination date of {@link String} type.
   * @return Number of days between the dates.
   */
  public static int DaysSinceFromDate(String source, String destination) {
    DateTime sourceDate = getFormattedDateTime(source);
    DateTime destinationDate = getFormattedDateTime(destination);
    return Days.daysBetween(sourceDate, destinationDate).getDays();
  }

  /**
   * Returns the number of days in the month in the given base date.
   *
   * @param date date of {@link DateTime} type.
   * @return number of days in the month in the given base date.
   */
  public static int DaysInMonth(DateTime date) {
    return date.dayOfMonth().getMaximumValue();
  }

  /**
   * Returns the number of days in the month in the given base date.
   *
   * @param date date of {@link String} type.
   * @return  number of days in the month in the given base date.
   */
  public static int DaysInMonth(String date) {
    return getFormattedDateTime(date).dayOfMonth().getMaximumValue();
  }

  /**
   * Returns the number of days in the year in the given base date.
   *
   * @param date date of {@link DateTime} type.
   * @return number of days in the year in the given base date.
   */
  public static int DaysInYear(DateTime date) {
    return date.dayOfYear().getMaximumValue();
  }

  /**
   * Returns the number of days in the year in the given base date.
   *
   * @param date date of {@link String} type.
   * @return number of days in the year in the given base date.
   */
  public static int DaysInYear(String date) {
    return getFormattedDateTime(date).dayOfYear().getMaximumValue();
  }

  public static DateTime DateOffsetByDays(DateTime date, int daysOffset) {
    return date.plusDays(daysOffset);
  }

  public static int HoursFromTime(DateTime date) {
    return date.getHourOfDay();
  }

  public static long JulianDayFromDate(DateTime date){
    return DateTimeUtils.toJulianDayNumber(date.getMillis());
  }

  public static long MicroSecondsFromTime(DateTime date) {
    return date.getMillis();
  }

  public static int MidnightSecondsFromTime(DateTime date) {
    return 0;
  }
}
