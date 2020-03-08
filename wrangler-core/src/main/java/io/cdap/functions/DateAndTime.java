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

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.Locale;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column datetime:CurrentDate()
 * set-column :column exp:{ datetime:CurrrentDate()  }
 */
public final class DateAndTime {
  private static final DateTimeFormatter DATE_TIME_FORMAT =
    new DateTimeFormatterBuilder()
      .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
      .appendLiteral("-")
      .appendValue(ChronoField.MONTH_OF_YEAR, 2)
      .appendLiteral("-")
      .appendValue(ChronoField.DAY_OF_MONTH, 2)
      .optionalStart()
      .appendLiteral(" ")
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .appendOptional(
        new DateTimeFormatterBuilder()
          .appendLiteral('.')
          .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, false)
          .toFormatter()
      )
      .toFormatter();

  private static final DateTimeFormatter TIME_FORMAT =
    new DateTimeFormatterBuilder()
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .appendLiteral(':')
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .appendOptional(
        new DateTimeFormatterBuilder()
          .appendLiteral('.')
          .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, false)
          .toFormatter()
      ).toFormatter();

  private static final DateTimeFormatter OUTPUT_DATETIME_FORMAT
    = DateTimeFormatter.ofPattern("HH:mm:ss dd MMM yyyy");

  private static LocalDate getDate(String date) {
    return LocalDate.parse(date, DATE_TIME_FORMAT);
  }

  private static LocalTime getTime(String time) {
    return LocalTime.parse(time, TIME_FORMAT);
  }

  private static LocalDateTime getTimestamp(String date) {
    return LocalDateTime.parse(date, DATE_TIME_FORMAT);
  }

  private static DayOfWeek getDayOfWeek(String textDay) {
    DayOfWeek dayOfWeek = DayOfWeek.SUNDAY;
    switch(textDay.toUpperCase()) {
      case "MON":
      case "MONDAY":
        dayOfWeek = DayOfWeek.MONDAY;
        break;

      case "TUE":
      case "TUESDAY":
        dayOfWeek = DayOfWeek.TUESDAY;
        break;

      case "WED":
      case "WEDNESDAY":
        dayOfWeek = DayOfWeek.WEDNESDAY;
        break;

      case "THU":
      case "THURSDAY":
        dayOfWeek = DayOfWeek.THURSDAY;
        break;

      case "FRI":
      case "FRIDAY":
        dayOfWeek = DayOfWeek.FRIDAY;
        break;

      case "SAT":
      case "SATUDAY":
        dayOfWeek = DayOfWeek.SATURDAY;
        break;

      case "SUN":
      case "SUNDAY":
        dayOfWeek = DayOfWeek.SUNDAY;
        break;
    }
    return dayOfWeek;
  }

  /**
   * @return returns current date with time.
   */
  public static LocalDate CurrentDate() {
    return LocalDate.now();
  }

  /**
   * @return return current localtime.
   */
  public static LocalTime CurrentTime() {
    return LocalTime.now();
  }

  /**
   * @return return current time in milliseconds.
   */
  public static long CurrentTimeMS() {
    return CurrentTime().getNano() / 1000000L;
  }

  /**
   * @return return current data & time.
   */
  public static LocalDateTime CurrentTimestamp() {
    return LocalDateTime.now();
  }

  /**
   * @return return current epoch seconds.
   */
  public static long EpochSeconds() {
    return Instant.now().getEpochSecond();
  }

  /**
   * @return return current data & time in milliseconds.
   */
  public static long CurrentTimestampMS() {
    return Instant.now().toEpochMilli();
  }

  /**
   * @return return current data & time in Nanoseconds.
   */
  public static long CurrentTimestampNano() {
    return Instant.now().getNano();
  }

  /**
   * Returns a date object by adding an integer to a current date.
   *
   * @param days to be added to current date.
   * @return returns a {@link LocalDate} by adding days to current date.
   */
  public static LocalDate DateFromDaysSince(int days) {
    LocalDate baseline = LocalDate.now();
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
  public static LocalDate DateFromDaysSince(int days, String baseline) {
    LocalDate baselineDate = getDate(baseline);
    // DateTime.parse(baseline, DateTimeFormat.forPattern(DATE_FORMAT));
    return baselineDate.plusDays(days);
  }

  /**
   * Creates a {@link LocalDate} from the components year, month, and day.
   *
   * @param year to used for date.
   * @param month to be used for date.
   * @param day to be used for date
   * @return a instance of {@link LocalDate}
   */
  public static LocalDate DateFromComponents(int year, int month, int day) {
    return LocalDate.of(year, month, day);
  }

  /**
   * Returns a date from the given Julian day number.
   *
   * @param julianDay Julian day number
   * @return a instance of {@link LocalDate} that represents the Julian day.
   */
  public static LocalDate DateFromJulianDay(int julianDay) {
    DateTime datetime = new DateTime(DateTimeUtils.fromJulianDay(julianDay));
    return LocalDate.of(datetime.getYear(), datetime.getMonthOfYear(), datetime.getDayOfMonth());
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
  public static LocalDate DateOffsetByComponents(LocalDate baseline, int yearOffset, int monthOffset, int daysOffset) {
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
  public static LocalDate DateOffsetByComponents(String baseline, int yearOffset, int monthOffset, int daysOffset) {
    return DateOffsetByComponents(getDate(baseline), yearOffset, monthOffset, daysOffset);
  }

  /**
   * Returns the number of days from the source date to the given date.
   * @param source date of {@link LocalDate} type.
   * @param destination date of {@link LocalDate} type.
   * @return Number of days between the dates.
   */
  public static long DaysSinceFromDate(LocalDate source, LocalDate destination) {
    return ChronoUnit.DAYS.between(source, destination);
  }

  /**
   * Returns the number of days from the source date to the given date.
   *
   * @param source date of {@link LocalDate} type.
   * @param destination date of {@link String} type.
   * @return Number of days between the dates.
   */
  public static long DaysSinceFromDate(LocalDate source, String destination) {
    return DaysSinceFromDate(source, getDate(destination));
  }

  /**
   * Returns the number of days from the source date to the given date.
   *
   * @param source date of {@link String} type.
   * @param destination date of {@link LocalDate} type.
   * @return Number of days between the dates.
   */
  public static long DaysSinceFromDate(String source, LocalDate destination) {
    return DaysSinceFromDate(getDate(source), destination);
  }

  /**
   * Returns the number of days from the source date to the given date.
   *
   * @param source date of {@link String} type.
   * @param destination date of {@link String} type.
   * @return Number of days between the dates.
   */
  public static long DaysSinceFromDate(String source, String destination) {
    LocalDate sourceDate = getDate(source);
    LocalDate destinationDate = getDate(destination);
    return DaysSinceFromDate(sourceDate, destinationDate);
  }

  /**
   * Returns the number of days in the month in the given base date.
   *
   * @param date date of {@link LocalDate} type.
   * @return number of days in the month in the given base date.
   */
  public static int DaysInMonth(LocalDate date) {
    return date.getMonth().maxLength();
  }

  /**
   * Returns the number of days in the month in the given base date.
   *
   * @param date date of {@link String} type.
   * @return  number of days in the month in the given base date.
   */
  public static int DaysInMonth(String date) {
    return DaysInMonth(getDate(date));
  }

  /**
   * Returns the number of days in the year in the given base date.
   *
   * @param date date of {@link LocalDate} type.
   * @return number of days in the year in the given base date.
   */
  public static int DaysInYear(LocalDate date) {
    return Year.of(date.getYear()).length();
  }

  /**
   * Returns the number of days in the year in the given base date.
   *
   * @param date date of {@link String} type.
   * @return number of days in the year in the given base date.
   */
  public static int DaysInYear(String date) {
    return DaysInYear(getDate(date));
  }

  /**
   * Returns the given date offset by the given number of days. The offset value can be positive, zero, or negative.
   *
   * @param date date of {@link LocalDate} type.
   * @param daysOffset days to offset the baseline date.
   * @return Date offset by days.
   */
  public static LocalDate DateOffsetByDays(LocalDate date, int daysOffset) {
    return date.plusDays(daysOffset);
  }

  /**
   * Returns the given date offset by the given number of days. The offset value can be positive, zero, or negative.
   *
   * @param date date of {@link String} type.
   * @param daysOffset days to offset the baseline date.
   * @return Date offset by days.
   */
  public static LocalDate DateOffsetByDays(String date, int daysOffset) {
    return DateOffsetByDays(getDate(date), daysOffset);
  }

  /**
   * Returns the hours portion of a time.
   *
   * @param time of {@link LocalTime} type.
   * @return hour portion of time
   */
  public static int HoursFromTime(LocalTime time) {
    return time.getHour();
  }

  /**
   * Returns the hours portion of a time.
   *
   * @param time of {@link String} type.
   * @return hour portion of time
   */
  public static int HoursFromTime(String time) {
    return HoursFromTime(getTime(time));
  }

  /**
   * Returns a Julian day number from the given date.
   *
   * @param date of {@link LocalDate} type.
   * @return Julian day number.
   */
  public static long JulianDayFromDate(LocalDate date){
    long l = date.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    return DateTimeUtils.toJulianDayNumber(l);
  }

  /**
   * Returns a Julian day number from the given date.
   *
   * @param date of {@link String} type.
   * @return Julian day number.
   */
  public static long JulianDayFromDate(String date) {
    return JulianDayFromDate(getDate(date));
  }

  /**
   * Returns the Nanosecond portion of a time.
   *
   * @param time of {@link LocalTime} type.
   * @return nanosecond portion of time.
   */
  public static int NanoSecondsFromTime(LocalTime time) {
    return time.getNano();
  }

  /**
   * Returns the nanosecond portion of a time.
   *
   * @param time of {@link String} type.
   * @return nanosecond portion of time.
   */
  public static int NanoSecondsFromTime(String time) {
    return NanoSecondsFromTime(getTime(time));
  }

  /**
   * Returns the microsecond portion of a time.
   *
   * @param time of {@link LocalTime} type.
   * @return microsecond portion of time.
   */
  public static int MicroSecondsFromTime(LocalTime time) {
    return NanoSecondsFromTime(time)/1000;
  }

  /**
   * Returns the microsecond portion of a time.
   *
   * @param time of {@link String} type.
   * @return microsecond portion of time.
   */
  public static int MicroSecondsFromTime(String time) {
    return MicroSecondsFromTime(getTime(time));
  }

  /**
   * Returns the millisecond portion of a time.
   *
   * @param time of {@link LocalTime} type.
   * @return millisecond portion of time.
   */
  public static int MilliSecondsFromTime(LocalTime time) {
    return MicroSecondsFromTime(time)/1000;
  }

  /**
   * Returns the millisecond portion of a time.
   *
   * @param time of {@link String} type.
   * @return millisecond portion of time.
   */
  public static int MilliSecondsFromTime(String time) {
    return MilliSecondsFromTime(getTime(time));
  }

  /**
   * Returns the number of seconds from midnight to the given time.
   *
   * @param time of {@link LocalTime} type.
   * @return seconds from midnight
   */
  public static int MidnightSecondsFromTime(LocalTime time) {
    return time.toSecondOfDay();
  }

  /**
   * Returns the number of seconds from midnight to the given time.
   *
   * @param time of {@link String} type.
   * @return seconds from midnight
   */
  public static int MidnightSecondsFromTime(String time) {
    return MidnightSecondsFromTime(getTime(time));
  }

  /**
   * Returns the minutes portion of a time.
   *
   * @param time of {@link LocalTime} type.
   * @return minute of time.
   */
  public static int MinutesFromTime(LocalTime time) {
    return time.getMinute();
  }

  /**
   * Returns the minutes portion of a time.
   *
   * @param time of {@link String} type.
   * @return minute of time.
   */
  public static int MinutesFromTime(String time) {
    return MinutesFromTime(getTime(time));
  }

  /**
   * Returns the day of the month from the given date.
   *
   * @param date of {@link LocalDate} type.
   * @return month of give date.
   */
  public static int MonthDayFromDate(LocalDate date) {
    return date.getDayOfMonth();
  }

  /**
   * Returns the day of the month from the given date.
   *
   * @param date of {@link LocalDate} type.
   * @return month of give date.
   */
  public static int MonthDayFromDate(String date) {
    return MonthDayFromDate(getDate(date));
  }

  /**
   * Returns the month number from the given date.
   *
   * @param date of {@link LocalDate} type.
   * @return  month value in date.
   */
  public static int MonthFromDate(LocalDate date) {
    return date.getMonthValue();
  }

  /**
   * Returns the month number from the given date.
   *
   * @param date of {@link String} type.
   * @return month value in date.
   */
  public static int MonthFromDate(String date) {
    return MonthFromDate(getDate(date));
  }

  /**
   * Returns the date of the specified day of the week soonest after the source date.
   * The day of the week is specified as the full name, for example, thursday, or a
   * three-letter abbreviation, for example, thu.
   *
   * @param source of {@link LocalDate} type.
   * @param textDayOfWeek text day of the week.
   * @return earliest day from the date.
   */
  public static LocalDate NextWeekdayFromDate(LocalDate source, String textDayOfWeek) {
    return source.with(TemporalAdjusters.next(getDayOfWeek(textDayOfWeek)));
  }

  /**
   * Returns the date of the specified day of the week soonest after the source date.
   * The day of the week is specified as the full name, for example, thursday, or a
   * three-letter abbreviation, for example, thu.
   *
   * @param source of {@link LocalDate} type.
   * @param textDayOfWeek text day of the week.
   * @return earliest day from the date.
   */
  public static LocalDate NextWeekdayFromDate(String source, String textDayOfWeek) {
    return NextWeekdayFromDate(getDate(source), textDayOfWeek);
  }

  /**
   * Returns the date of the specified day of the week offset by the specified number of weeks from the source date.
   * The day of the week is specified as the full name, for example, thursday, or a three-letter abbreviation,
   * for example, thu. The offset values can be positive, negative, or zero.
   *
   * @param source of {@link LocalDate} type.
   * @param textDayOfWeek Display week of day string
   * @param offset offset week.
   * @return date of the specified day of the week offset
   */
  public static LocalDate NthWeekdayFromDate(LocalDate source, String textDayOfWeek, int offset) {
    LocalDate target = source;
    if (offset > 0) {
      for (int i = 0; i < offset; i++) {
        target = target.with(TemporalAdjusters.next(getDayOfWeek(textDayOfWeek)));
      }
    } else if (offset < 0){
      for (int i = offset; i < 0; i++) {
        target = target.with(TemporalAdjusters.previous(getDayOfWeek(textDayOfWeek)));
      }
    } else if (offset == 0) {
      target = target.with(TemporalAdjusters.next(getDayOfWeek(textDayOfWeek)));
    }
    return target;
  }

  /**
   * Returns the date of the specified day of the week offset by the specified number of weeks from the source date.
   * The day of the week is specified as the full name, for example, thursday, or a three-letter abbreviation,
   * for example, thu. The offset values can be positive, negative, or zero.
   *
   * @param source of {@link String} type.
   * @param textDayOfWeek Display week of day string
   * @param offset offset week.
   * @return date of the specified day of the week offset
   */
  public static LocalDate NthWeekdayFromDate(String source, String textDayOfWeek, int offset) {
    return NthWeekdayFromDate(getDate(source), textDayOfWeek, offset);
  }

  /**
   * Returns the date of the specified day of the week that is the most recent day before the source date.
   * The day of the week is specified as the full name, for example, thursday, or a three-letter abbreviation,
   * for example, thu.
   *
   * @param source of {@link LocalDate} type.
   * @param textDayOfWeek Display week of day string
   * @return previous day of the week.
   */
  public static LocalDate PreviousWeekdayFromDate(LocalDate source, String textDayOfWeek) {
    return source.with(TemporalAdjusters.previous(getDayOfWeek(textDayOfWeek)));
  }

  /**
   * Returns the date of the specified day of the week that is the most recent day before the source date.
   * The day of the week is specified as the full name, for example, thursday, or a three-letter abbreviation,
   * for example, thu.
   *
   * @param source of {@link String} type.
   * @param textDayOfWeek Display week of day string
   * @return previous day of the week.
   */
  public static LocalDate PreviousWeekdayFromDate(String source, String textDayOfWeek) {
    return PreviousWeekdayFromDate(getDate(source), textDayOfWeek);
  }

  /**
   * Returns the seconds portion of a time.
   *
   * @param time of {@link LocalTime} type.
   * @return second portion of time.
   */
  public static int SecondsFromTime(LocalTime time) {
    return time.getSecond();
  }

  /**
   * Returns the seconds portion of a time.
   *
   * @param time of {@link String} type.
   * @return second portion of time.
   */
  public static int SecondsFromTime(String time) {
    return SecondsFromTime(getTime(time));
  }

  /**
   * Returns the number of seconds between two time stamp objects.
   *
   * @param source a {@link LocalDateTime} type
   * @param destination a {@link LocalDateTime} type
   * @return number of seconds between two timestamps.
   */
  public static long SecondsSinceFromTimestamp(LocalDateTime source, LocalDateTime destination) {
    return Duration.between(destination, source).getSeconds();
  }

  /**
   * Returns the number of seconds between two time stamp objects.
   *
   * @param source a {@link String} type
   * @param destination a {@link LocalDateTime} type
   * @return number of seconds between two timestamps.
   */
  public static long SecondsSinceFromTimestamp(String source, LocalDateTime destination) {
    return SecondsSinceFromTimestamp(getTimestamp(source), destination);
  }

  /**
   * Returns the number of seconds between two time stamp objects.
   *
   * @param source a {@link LocalDateTime} type
   * @param destination a {@link String} type
   * @return number of seconds between two timestamps.
   */
  public static long SecondsSinceFromTimestamp(LocalDateTime source, String destination) {
    return SecondsSinceFromTimestamp(source, getTimestamp(destination));
  }

  /**
   * Returns the number of seconds between two time stamp objects.
   *
   * @param source a {@link String} type
   * @param destination a {@link String} type
   * @return number of seconds between two timestamps.
   */
  public static long SecondsSinceFromTimestamp(String source, String destination) {
    return SecondsSinceFromTimestamp(getTimestamp(source), getTimestamp(destination));
  }

  /**
   * @return Returns the system time and date as a formatted string.
   */
  public static String TimeDate() {
    LocalDateTime now = LocalDateTime.now();
    return now.format(OUTPUT_DATETIME_FORMAT);
  }

  /**
   * Returns a time from the given hours, minutes, seconds, and microseconds, given as four separate values.
   *
   * @param hours component of {@link LocalTime}
   * @param mins component of {@link LocalTime}
   * @param seconds component of {@link LocalTime}
   * @param nanoseconds component of {@link LocalTime}
   * @return a instance of {@link LocalTime}
   */
  public static LocalTime TimeFromComponents(int hours, int mins, int seconds, int nanoseconds) {
    return LocalTime.of(hours, mins, seconds, nanoseconds);
  }

  /**
   * Returns the time given the number of seconds since midnight.
   *
   * @param seconds since midnight.
   * @return time since midnight.
   */
  public static LocalTime TimeFromMidnightSeconds(int seconds) {
    return LocalTime.of(0,0,0).plusSeconds(seconds);
  }

  /**
   * Returns the time given the number of seconds since midnight.
   *
   * @param seconds since midnight.
   * @return time since midnight.
   */
  public static LocalTime TimeFromMidnightSeconds(String seconds) {
    return LocalTime.of(0,0,0).plusSeconds(Integer.parseInt(seconds));
  }

  /**
   * Returns the time, with offsets applied from the base time.
   * hour offset, minute offset, and second offset each given as separate values.
   * The seconds offset can include partial seconds.
   *
   * @param time baseline {@link LocalTime}
   * @param hour offset to added to baseline.
   * @param min offset to added to baseline.
   * @param seconds offset to added to baseline.
   * @return hour, min and second added to baseline.
   */
  public static LocalTime TimeOffsetByComponents(LocalTime time, int hour, int min, int seconds) {
    return time.plusHours(hour).plusMinutes(min).plusSeconds(seconds);
  }

  /**
   * Returns the time, with offsets applied from the base time.
   * hour offset, minute offset, and second offset each given as separate values.
   * The seconds offset can include partial seconds.
   *
   * @param time baseline {@link String}
   * @param hour offset to added to baseline.
   * @param min offset to added to baseline.
   * @param seconds offset to added to baseline.
   * @return hour, min and second added to baseline.
   */
  public static LocalTime TimeOffsetByComponents(String time, int hour, int min, int seconds) {
    return TimeOffsetByComponents(getTime(time), hour, min, seconds);
  }

  /**
   * Returns a time stamp from the given date and time.
   *
   * @param date a instance of {@link LocalDate}
   * @param time a instance of {@link LocalTime}
   * @return a instance of {@link LocalDateTime}
   */
  public static LocalDateTime TimestampFromDateTime(LocalDate date, LocalTime time) {
    return LocalDateTime.of(date, time);
  }

  /**
   * Returns a time stamp from the given date and time.
   *
   * @param date a instance of {@link String}
   * @param time a instance of {@link LocalTime}
   * @return a instance of {@link LocalDateTime}
   */
  public static LocalDateTime TimestampFromDateTime(String date, LocalTime time) {
    return TimestampFromDateTime(getDate(date), time);
  }

  /**
   * Returns a time stamp from the given date and time.
   *
   * @param date a instance of {@link LocalDate}
   * @param time a instance of {@link String}
   * @return a instance of {@link LocalDateTime}
   */
  public static LocalDateTime TimestampFromDateTime(LocalDate date, String time) {
    return TimestampFromDateTime(date, getTime(time));
  }

  /**
   * Returns a time stamp from the given date and time.
   *
   * @param date a instance of {@link String}
   * @param time a instance of {@link String}
   * @return a instance of {@link LocalDateTime}
   */
  public static LocalDateTime TimestampFromDateTime(String date, String time) {
    return TimestampFromDateTime(getDate(date), getTime(time));
  }

  /**
   * Returns a time stamp that is derived from the number of seconds from the base time stamp object.
   *
   * @param seconds to be added to base timestamp.
   * @param time base timestamp.
   * @return timestamp with seconds added to base time.
   */
  public static LocalDateTime TimestampFromSecondSince(int seconds, LocalDateTime time) {
    return time.plusSeconds(seconds);
  }

  /**
   * Returns a time stamp that is derived from the number of seconds from the base time stamp object.
   *
   * @param seconds to be added to base timestamp.
   * @param time base timestamp.
   * @return timestamp with seconds added to base time.
   */
  public static LocalDateTime TimestampFromSecondSince(int seconds, String time) {
    return TimestampFromSecondSince(seconds, getTimestamp(time));
  }

  /**
   * Returns a time stamp that is derived from the number of seconds from the base time stamp object.
   *
   * @param seconds to be added to base timestamp.
   * @param time base timestamp.
   * @return timestamp with seconds added to base time.
   */
  public static LocalDateTime TimestampFromSecondSince(String seconds, LocalDateTime time) {
    return TimestampFromSecondSince(Integer.parseInt(seconds), time);
  }

  /**
   * Returns a time stamp that is derived from the number of seconds from the base time stamp object.
   *
   * @param seconds to be added to base timestamp.
   * @param time base timestamp.
   * @return timestamp with seconds added to base time.
   */
  public static LocalDateTime TimestampFromSecondSince(String seconds, String time) {
    return TimestampFromSecondSince(Integer.parseInt(seconds), getTimestamp(time));
  }

  /**
   * Returns a time stamp from the given epoch time specified in seconds.
   *
   * @param epochTimestamp specified in seconds.
   * @return a instance {@link LocalDateTime}
   */
  public static LocalDateTime TimestampFromTimet(long epochTimestamp) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochTimestamp*1000), ZoneId.of("UTC"));
  }

  /**
   * Returns a time stamp from the given time and timestamp objects.
   * The value in the time object overwrites the time value in the time stamp object
   * so that only the date part is used from the time stamp.
   *
   * @param time to replace in timestamp
   * @param timestamp who's timestamp has to replaced.
   * @return modified timestamp.
   */
  public static LocalDateTime TimestampFromTime2(LocalTime time, LocalDateTime timestamp) {
    return timestamp
      .withHour(time.getHour())
      .withMinute(time.getMinute())
      .withSecond(time.getSecond())
      .withNano(time.getNano());
  }

  /**
   * Returns a time stamp from the given time and timestamp objects.
   * The value in the time object overwrites the time value in the time stamp object
   * so that only the date part is used from the time stamp.
   *
   * @param time to replace in timestamp
   * @param timestamp who's timestamp has to replaced.
   * @return modified timestamp.
   */
  public static LocalDateTime TimestampFromTime2(LocalTime time, String timestamp) {
    return TimestampFromTime2(time, getTimestamp(timestamp));
  }

  /**
   * Returns a time stamp from the given time and timestamp objects.
   * The value in the time object overwrites the time value in the time stamp object
   * so that only the date part is used from the time stamp.
   *
   * @param time to replace in timestamp
   * @param timestamp who's timestamp has to replaced.
   * @return modified timestamp.
   */
  public static LocalDateTime TimestampFromTime2(String time, LocalDateTime timestamp) {
    return TimestampFromTime2(getTime(time), timestamp);
  }

  /**
   * Returns a time stamp from the given time and timestamp objects.
   * The value in the time object overwrites the time value in the time stamp object
   * so that only the date part is used from the time stamp.
   *
   * @param time to replace in timestamp
   * @param timestamp who's timestamp has to replaced.
   * @return modified timestamp.
   */
  public static LocalDateTime TimestampFromTime2(String time, String timestamp) {
    return TimestampFromTime2(getTime(time), getTimestamp(timestamp));
  }

  /**
   * @return Returns the time stamp, with offsets applied from the base time stamp.
   */
  public static LocalDateTime TimestampOffsetByComponents(LocalDateTime timestamp, long yearOffset,
                                                          long monthOffset, long dayOfMonthOffset,
                                                          long hourOffset, long minOffset, long secondOffset) {
    return timestamp
      .plusYears(yearOffset)
      .plusMonths(monthOffset)
      .plusDays(dayOfMonthOffset)
      .plusHours(hourOffset)
      .plusMinutes(minOffset)
      .plusSeconds(secondOffset);
  }

  /**
   * @return Returns the time stamp, with offsets applied from the base time stamp.
   */
  public static LocalDateTime TimestampOffsetByComponents(String timestamp, int yearOffset,
                                                          int monthOffset, int dayOfMonthOffset,
                                                          int hourOffset, int minOffset, int secondOffset) {
    return TimestampOffsetByComponents(getTimestamp(timestamp), yearOffset, monthOffset,
                                       dayOfMonthOffset, hourOffset, minOffset, secondOffset);
  }

  /**
   * @return Returns the time stamp, with offsets applied from the base time stamp.
   */
  public static LocalDateTime TimestampOffsetByComponents(String timestamp, long yearOffset,
                                                          long monthOffset, long dayOfMonthOffset,
                                                          long hourOffset, long minOffset, long secondOffset) {
    return TimestampOffsetByComponents(getTimestamp(timestamp), yearOffset, monthOffset,
                                       dayOfMonthOffset, hourOffset, minOffset, secondOffset);
  }

  /**
   * @return Returns the time stamp, with offsets applied from the base time stamp with seconds offset.
   */
  public static LocalDateTime TimestampOffsetBySeconds(LocalDateTime timestamp, long secondsOffset) {
    return timestamp.plusSeconds(secondsOffset);
  }

  /**
   * @return Returns the time stamp, with offsets applied from the base time stamp with seconds offset.
   */
  public static LocalDateTime TimestampOffsetBySeconds(String timestamp, long secondsOffset) {
    return TimestampOffsetBySeconds(getTimestamp(timestamp), secondsOffset);
  }

  /**
   * @return Returns a UNIX epoch time from the given time stamp.
   */
  public static long EpochFromTimestamp(LocalDateTime timestamp) {
    return timestamp.toEpochSecond(ZoneOffset.UTC);
  }

  /**
   * @return Returns a UNIX epoch time from the given time stamp.
   */
  public static long EpochFromTimestamp(String timestamp) {
    return EpochFromTimestamp(getTimestamp(timestamp));
  }

  /**
   * @return Returns the day number of the week from the given date.
   */
  public static int WeekdayFromDate(LocalDate date, String startOfWeek) {
    return date.with(
      TemporalAdjusters.previousOrSame(
        DayOfWeek.valueOf(
          startOfWeek.toUpperCase()
        )
      )
    ).atStartOfDay(ZoneId.of("UTC")).getDayOfWeek().getValue();
  }
  /**
   * @return Returns the day number of the week from the given date.
   */
  public static int WeekdayFromDate(String date, String startOfWeek) {
    return WeekdayFromDate(getDate(date), startOfWeek);
  }

  /**
   * @return Returns the day number of the week from the given date.
   */
  public static int WeekdayFromDate(LocalDate date) {
    return WeekdayFromDate(date, "Sunday");
  }

  /**
   * @return Returns the day number of the week from the given date.
   */
  public static int WeekdayFromDate(String date) {
    return WeekdayFromDate(getDate(date));
  }

  /**
   * @return Returns the day number in the year from the given date.
   */
  public static int YeardayFromDate(LocalDate date) {
    return date.getDayOfYear();
  }

  /**
   * @return Returns the day number in the year from the given date.
   */
  public static int YeardayFromDate(String date) {
    return YeardayFromDate(getDate(date));
  }

  /**
   * @return Returns the year from the given date.
   */
  public static int YearFromDate(LocalDate date) {
    return date.getYear();
  }

  /**
   * @return Returns the year from the given date.
   */
  public static int YearFromDate(String date) {
    return YearFromDate(getDate(date));
  }

  /**
   * @return Returns the week number in the year from the given date.
   */
  public static int YearweekFromDate(LocalDate date) {
    TemporalField woy = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();
    return date.get(woy);
  }

  /**
   * @return Returns the week number in the year from the given date.
   */
  public static int YearweekFromDate(String date) {
    return YearweekFromDate(getDate(date));
  }
}
