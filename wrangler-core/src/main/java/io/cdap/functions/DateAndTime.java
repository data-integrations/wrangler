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

import io.cdap.functions.ibm.StringFunc;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Days;
import org.joda.time.LocalTime;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column datetime:CurrentDate()
 * set-column :column exp:{ datetime:CurrrentDate()  }
 */
public class DateAndTime {
  /**
   * @return returns current date with time.
   */
  public static DateTime CurrentDate() {
    return new org.joda.time.DateTime();
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
  public static DateTime CurrentTimeStamp() {
    return new DateTime();
  }

  /**
   * @return return current data & time in milliseconds.
   */
  public static long CurrentTimeStampMS() {
    return new DateTime().getMillis();
  }

  /**
   * Returns a date object by adding an integer to a current date
   *
   * @param days to be added to current date.
   * @return returns a {@link DateTime} by adding days to current date.
   */
  public static DateTime DateFromDaysSince2(long days) {
    DateTime dt = new DateTime();
    return dt.plus(days);
  }

  public static DateTime DateFromDaysSince2(long days, StringFunc date) {
    DateTime dt = new DateTime(date);
    return dt.plus(days);
  }

  public static DateTime DateFromDaySince(long days) {
    return DateFromDaysSince2(days);
  }

  public static DateTime DateFromDaySince(long days, StringFunc date) {
    return DateFromDaysSince2(days, date);
  }

  public static DateTime DateFromComponents(int year, int month, int dayofmonth) {
    return new DateTime(year, month, dayofmonth, 0, 0);
  }

  /**
   * Returns a date from the given Julian day number.
   *
   * @param day Julian day number
   * @return a instance of {@link DateTime} that represents the Julian day.
   */
  public static DateTime DateFromJulianDay(int day) {
    long l = DateTimeUtils.fromJulianDay(day);
    return new DateTime(l);
  }

  public static DateTime DateOffsetByComponents(DateTime date, int yearOffset, int monthOffset, int daysOffset) {
    return date.plusYears(yearOffset).plusMonths(monthOffset).plusDays(daysOffset);
  }

  public static int DaysSinceFromDate2(DateTime source, DateTime destination) {
    return Days.daysBetween(source, destination).getDays();
  }

  public static int DaysSinceFromDate(DateTime source, StringFunc destination) {
    return Days.daysBetween(source, new DateTime(destination)).getDays();
  }

  public static int DaysInMonth(DateTime date) {
    return date.dayOfMonth().getMaximumValue();
  }

  public static int DaysInYear(DateTime date) {
    return date.dayOfYear().getMaximumValue();
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
