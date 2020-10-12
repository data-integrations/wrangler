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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column datetime:CurrentDate()
 * set-column :column exp:{ datetime:CurrrentDate() }
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
   * Return the {@link LocalDate} the string represents. If the string is null, this method will return null.
   */
  public static LocalDate GetDate(String date) {
    return date == null ? null : LocalDate.parse(date, DATE_TIME_FORMAT);
  }

  /**
   * Return the {@link LocalTime} the string represents. If the string is null, this method will return null.
   */
  public static LocalTime GetTime(String time) {
    return time == null ? null : LocalTime.parse(time, TIME_FORMAT);
  }

  /**
   * Return the {@link LocalDateTime} the string represents. If the string is null, this method will return null.
   */
  public static LocalDateTime GetDateTime(String date) {
    return LocalDateTime.parse(date, DATE_TIME_FORMAT);
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
  public static LocalDateTime CurrentDateTime() {
    return LocalDateTime.now();
  }

  /**
   * @return return current timestamp in default timezone.
   */
  public static ZonedDateTime CurrentTimestamp() {
    return ZonedDateTime.now();
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
   * Returns a date object by adding an integer to a current date. If this integer is null, current date will
   * be returned. The integer can be negative to return a date that is earlier than the current date.
   *
   * @param days to be added to current date.
   * @return returns a {@link LocalDate} by adding days to current date.
   */
  public static LocalDate DateFromDaysSince(Integer days) {
    return DateFromDaysSince(days, LocalDate.now());
  }

  /**
   * Returns a date object by adding an integer to a baseline date. If this integer is null, baseline date will be
   * returned. The integer can be negative to return a date that is earlier than the baseline date.
   * If baseline is null, the current date will be used. If days is null, it will be treated as 0.
   *
   * @param days to be added to the baseline date.
   * @param baseline date to which 'days' are added.
   * @return days added positive or negative to the baseline date.
   */
  public static LocalDate DateFromDaysSince(Integer days, LocalDate baseline) {
    baseline = baseline == null ? LocalDate.now() : baseline;
    return baseline.plusDays(days == null ? 0 : days);
  }

  /**
   * Creates a {@link LocalDate} from the components year, month, and day. If any input is null, null will be
   * returned.
   *
   * @param year to used for date.
   * @param month to be used for date.
   * @param day to be used for date
   * @return a instance of {@link LocalDate}
   */
  public static LocalDate DateFromComponents(Integer year, Integer month, Integer day) {
    return year == null || month == null || day == null ? null : LocalDate.of(year, month, day);
  }

  /**
   * Returns a date from the given Julian day number. The day number will be treated as 0 if it is null.
   *
   * @param julianDay Julian day number
   * @return a instance of {@link LocalDate} that represents the Julian day.
   */
  public static LocalDate DateFromJulianDay(Long julianDay) {
    DateTime datetime = new DateTime(DateTimeUtils.fromJulianDay(julianDay == null ? 0d : julianDay.doubleValue()));
    return LocalDate.of(datetime.getYear(), datetime.getMonthOfYear(), datetime.getDayOfMonth());
  }

  /**
   * Returns the given date, with offsets applied from the given year offset, month offset, and day of month offset.
   * offsets are given as three separate values. The offset values can each be positive, zero, or negative.
   * If base line is null, null will be returned. If the offset is null, it will be treated as 0.
   *
   * @param baseline baseline date.
   * @param yearOffset year offset to be applied.
   * @param monthOffset month offset to be applied.
   * @param daysOffset days offset to be applied.
   * @return Modified date with offsets applied.
   */
  public static LocalDate DateOffsetByComponents(LocalDate baseline, Integer yearOffset,
                                                 Integer monthOffset, Integer daysOffset) {
    return baseline == null ? null : baseline.plusYears(yearOffset == null ? 0 : yearOffset.longValue())
                                       .plusMonths(monthOffset == null ? 0 : monthOffset.longValue())
                                       .plusDays(daysOffset == null ? 0 : daysOffset.longValue());
  }

  /**
   * Returns the number of days from the source date to the given date. If any date is null, null will be returned.
   *
   * @param source date of {@link LocalDate} type.
   * @param destination date of {@link LocalDate} type.
   * @return Number of days between the dates.
   */
  public static Long DaysSinceFromDate(LocalDate source, LocalDate destination) {
    return source == null || destination == null ? null : ChronoUnit.DAYS.between(source, destination);
  }

  /**
   * Returns the number of days in the month in the given base date. If date is null, null will be returned.
   *
   * @param date date of {@link LocalDate} type.
   * @return number of days in the month in the given base date.
   */
  public static Integer DaysInMonth(LocalDate date) {
    return date == null ? null : date.getMonth().maxLength();
  }

  /**
   * Returns the number of days in the year in the given base date. If date is null, null will be returned.
   *
   * @param date date of {@link LocalDate} type.
   * @return number of days in the year in the given base date.
   */
  public static Integer DaysInYear(LocalDate date) {
    return date == null ? null : Year.of(date.getYear()).length();
  }

  /**
   * Returns the given date offset by the given number of days. The offset value can be positive, zero, or negative.
   * If date is null, null will be returned. If offset is null, it will be treated as 0.
   *
   * @param date date of {@link LocalDate} type.
   * @param daysOffset days to offset the baseline date.
   * @return Date offset by days.
   */
  public static LocalDate DateOffsetByDays(LocalDate date, Integer daysOffset) {
    return date == null ? null : date.plusDays(daysOffset == null ? 0 : daysOffset);
  }

  /**
   * Returns the hours portion of a time. If time is null, null will be returned.
   *
   * @param time of {@link LocalTime} type.
   * @return hour portion of time
   */
  public static Integer HoursFromTime(LocalTime time) {
    return time == null ? null : time.getHour();
  }

  /**
   * Returns a Julian day number from the given date. If date is null, null will be returned.
   *
   * @param date of {@link LocalDate} type.
   * @return Julian day number.
   */
  public static Long JulianDayFromDate(LocalDate date){
    if (date == null) {
      return null;
    }
    long l = date.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    return DateTimeUtils.toJulianDayNumber(l);
  }

  /**
   * Returns the Nanosecond portion of a time. If time is null, null will be returned.
   *
   * @param time of {@link LocalTime} type.
   * @return nanosecond portion of time.
   */
  public static Integer NanoSecondsFromTime(LocalTime time) {
    return time == null ? null : time.getNano();
  }

  /**
   * Returns the microsecond portion of a time. If time is null, null will be returned.
   *
   * @param time of {@link LocalTime} type.
   * @return microsecond portion of time.
   */
  public static Integer MicroSecondsFromTime(LocalTime time) {
    return NanoSecondsFromTime(time) / 1000;
  }

  /**
   * Returns the millisecond portion of a time. If time is null, null will be returned.
   *
   * @param time of {@link LocalTime} type.
   * @return millisecond portion of time.
   */
  public static Integer MilliSecondsFromTime(LocalTime time) {
    return MicroSecondsFromTime(time) / 1000;
  }

  /**
   * Returns the number of seconds from midnight to the given time. If time is null, null will be returned.
   *
   * @param time of {@link LocalTime} type.
   * @return seconds from midnight
   */
  public static Integer MidnightSecondsFromTime(LocalTime time) {
    return time == null ? null : time.toSecondOfDay();
  }

  /**
   * Returns the minutes portion of a time. If time is null, null will be returned.
   *
   * @param time of {@link LocalTime} type.
   * @return minute of time.
   */
  public static Integer MinutesFromTime(LocalTime time) {
    return time == null ? null : time.getMinute();
  }

  /**
   * Returns the day of the month from the given date. If date is null, null will be returned.
   *
   * @param date of {@link LocalDate} type.
   * @return month of give date.
   */
  public static Integer MonthDayFromDate(LocalDate date) {
    return date == null ? null : date.getDayOfMonth();
  }

  /**
   * Returns the month number from the given date. If date is null, null will be returned.
   *
   * @param date of {@link LocalDate} type.
   * @return  month value in date.
   */
  public static Integer MonthFromDate(LocalDate date) {
    return date == null ? null : date.getMonthValue();
  }

  /**
   * Returns the date of the specified day of the week soonest after the source date.
   * The day of the week is specified as the full name, for example, thursday, or a
   * three-letter abbreviation, for example, thu.
   * If source is null, null will be returned. If the day of the week is null, source will be returned.
   *
   * @param source of {@link LocalDate} type.
   * @param textDayOfWeek text day of the week.
   * @return earliest day from the date.
   */
  public static LocalDate NextWeekdayFromDate(LocalDate source, String textDayOfWeek) {
    return source == null ? null : (textDayOfWeek == null ? source :
                                      source.with(TemporalAdjusters.next(getDayOfWeek(textDayOfWeek))));
  }

  /**
   * Returns the date of the specified day of the week offset by the specified number of weeks from the source date.
   * The day of the week is specified as the full name, for example, thursday, or a three-letter abbreviation,
   * for example, thu. The offset values can be positive, negative, or zero.
   * If source is null, null will be returned. If textDayOfWeek is null, source will be returned.
   * If offset is null, it will be treated as 0.
   *
   * @param source of {@link LocalDate} type.
   * @param textDayOfWeek Display week of day string
   * @param offset offset week.
   * @return date of the specified day of the week offset
   */
  public static LocalDate NthWeekdayFromDate(LocalDate source, String textDayOfWeek, Number offset) {
    if (source == null || textDayOfWeek == null) {
      return source;
    }

    LocalDate target = source;
    int counter = offset == null ? 0 : Math.abs(offset.intValue());
    for (int i = 0; i < counter; i++) {
      target = target.with(offset.intValue() > 0 ? TemporalAdjusters.next(getDayOfWeek(textDayOfWeek)) :
                             TemporalAdjusters.previous(getDayOfWeek(textDayOfWeek)));
    }
    return target;
  }

  /**
   * Returns the date of the specified day of the week that is the most recent day before the source date.
   * The day of the week is specified as the full name, for example, thursday, or a three-letter abbreviation,
   * for example, thu.
   * If source is null, null will be returned. If the day of the week is null, source will be returned.
   *
   * @param source of {@link LocalDate} type.
   * @param textDayOfWeek Display week of day string
   * @return previous day of the week.
   */
  public static LocalDate PreviousWeekdayFromDate(LocalDate source, String textDayOfWeek) {
    return source == null ? null : (textDayOfWeek == null ? source :
                                     source.with(TemporalAdjusters.previous(getDayOfWeek(textDayOfWeek))));
  }

  /**
   * Returns the seconds portion of a time. If time is null, null will be returned.
   *
   * @param time of {@link LocalTime} type.
   * @return second portion of time.
   */
  public static Integer SecondsFromTime(LocalTime time) {
    return time == null ? null : time.getSecond();
  }

  /**
   * Returns the number of seconds between two datetime objects. If either datetime is null, null will be returned.
   *
   * @param source a {@link LocalDateTime} type
   * @param destination a {@link LocalDateTime} type
   * @return number of seconds between two datetimes.
   */
  public static Long SecondsSinceFromDateTime(LocalDateTime source, LocalDateTime destination) {
    return source == null || destination == null ? null : Duration.between(destination, source).getSeconds();
  }

  /**
   * @return Returns the system time and date as a formatted string.
   */
  public static String TimeDate() {
    return LocalDateTime.now().format(OUTPUT_DATETIME_FORMAT);
  }

  /**
   * Returns a time from the given hours, minutes, seconds, and microseconds, given as four separate values.
   * If any input is null, null will be returned.
   *
   * @param hours component of {@link LocalTime}
   * @param mins component of {@link LocalTime}
   * @param seconds component of {@link LocalTime}
   * @param microseconds component of {@link LocalTime}
   * @return a instance of {@link LocalTime}
   */
  public static LocalTime TimeFromComponents(Integer hours, Integer mins, Integer seconds, Integer microseconds) {
    return hours == null || mins == null || seconds == null || microseconds == null ?
             null : LocalTime.of(hours, mins, seconds, microseconds * 1000);
  }

  /**
   * Returns the time given the number of seconds since midnight. If seconds is null, midnight will be returned.
   *
   * @param seconds since midnight.
   * @return time since midnight.
   */
  public static LocalTime TimeFromMidnightSeconds(Integer seconds) {
    return LocalTime.of(0,0,0).plusSeconds(seconds == null ? 0 : seconds);
  }

  /**
   * Returns the time, with offsets applied from the base time.
   * hour offset, minute offset, and second offset each given as separate values.
   * The seconds offset can include partial seconds.
   * If time is null, null will be returned. If the offset is null, it will be treated as 0.
   *
   * @param time baseline {@link LocalTime}
   * @param hour offset to added to baseline.
   * @param min offset to added to baseline.
   * @param seconds offset to added to baseline.
   * @return hour, min and second added to baseline.
   */
  public static LocalTime TimeOffsetByComponents(LocalTime time, Integer hour, Integer min, Integer seconds) {
    return time == null ? null : time.plusHours(hour == null ? 0 : hour)
                                   .plusMinutes(min == null ? 0 : min)
                                   .plusSeconds(seconds == null ? 0 : seconds);
  }

  /**
   * Returns a datetime from the given date and time. If any input is null, null will be returned.
   *
   * @param date a instance of {@link LocalDate}
   * @param time a instance of {@link LocalTime}
   * @return a instance of {@link LocalDateTime}
   */
  public static LocalDateTime GetDateTime(LocalDate date, LocalTime time) {
    return date == null || time == null ? null : LocalDateTime.of(date, time);
  }

  /**
   * Returns a datetime that is derived from the number of seconds from the base datetime object.
   * If time is null, null will be returned. If seconds is null, time will be returned.
   *
   * @param seconds to be added to base datetime.
   * @param time base datetime.
   * @return datetime with seconds added to base time.
   */
  public static LocalDateTime DateTimeFromSecondsSince(Integer seconds, LocalDateTime time) {
    return time == null ? null : time.plusSeconds(seconds == null ? 0 : seconds);
  }

  /**
   * Returns a datetime from the given epoch time specified in seconds. This method assumes the timestamp is in UTC
   * time. If timestamp is null, null will be returned.
   *
   * @param epochTimestamp specified in seconds.
   * @return a instance {@link LocalDateTime}
   */
  public static LocalDateTime DateTimeFromEpoch(Long epochTimestamp) {
    return epochTimestamp == null ? null :
             LocalDateTime.ofInstant(Instant.ofEpochMilli(epochTimestamp * 1000), ZoneId.of("UTC"));
  }

  /**
   * Returns a datetime from the given time and datetime objects.
   * The value in the time object overwrites the time value in the datetime object
   * so that only the date part is used from the datetime.
   * If datetime is null, null will be returned. If time is null, datetime will be returned.
   *
   * @param time to replace in datetime
   * @param datetime who's datetime has to replaced.
   * @return modified datetime.
   */
  public static LocalDateTime DateTimeFromTime(LocalTime time, LocalDateTime datetime) {
    return datetime == null ? null : (time == null ? datetime : datetime
                                                                  .withHour(time.getHour())
                                                                  .withMinute(time.getMinute())
                                                                  .withSecond(time.getSecond())
                                                                  .withNano(time.getNano()));
  }

  /**
   * Returns a datetime from the given time and the offsets.
   * If datetime is null, null will be returned. If offset is null, it will be treated as 0.
   *
   * @return Returns the datetime, with offsets applied from the base datetime.
   */
  public static LocalDateTime DateTimeOffsetByComponents(LocalDateTime datetime, Integer yearOffset,
                                                         Integer monthOffset, Integer dayOfMonthOffset,
                                                         Integer hourOffset, Integer minOffset, Integer secondOffset) {
    return datetime == null ? null : datetime
                                       .plusYears(yearOffset == null ? 0 : yearOffset)
                                       .plusMonths(monthOffset == null ? 0 : monthOffset)
                                       .plusDays(dayOfMonthOffset == null ? 0 : dayOfMonthOffset)
                                       .plusHours(hourOffset == null ? 0 : hourOffset)
                                       .plusMinutes(minOffset == null ? 0 : minOffset)
                                       .plusSeconds(secondOffset == null ? 0 : secondOffset);
  }

  /**
   * @return Returns the datetime, with offsets applied from the base datetime with seconds offset.
   * If datetime is null, null will be returned. If seconds is null, it will be treated as 0.
   */
  public static LocalDateTime DateTimeOffsetBySeconds(LocalDateTime datetime, Integer secondsOffset) {
    return datetime == null ? null : datetime.plusSeconds(secondsOffset == null ? 0 : secondsOffset);
  }

  /**
   * @return Returns a UNIX epoch time in seconds from the given datetime in UTC time.
   * If datetime is null, null will be returned.
   */
  public static Long EpochFromDateTime(LocalDateTime datetime) {
    return datetime == null ? null : datetime.toEpochSecond(ZoneOffset.UTC);
  }

  /**
   * @return Returns a UNIX epoch time in seconds from the given timestamp in UTC time.
   * If timestamp is null, null will be returned.
   */
  public static Long EpochFromTimestamp(ZonedDateTime timestamp) {
    return timestamp == null ? null : timestamp.toEpochSecond();
  }

  /**
   * @return Returns the day number of the week from the given date with start of week provided.
   * If date is null, null will be returned. If startOfWeek is null, the date's day number of the week will be returned.
   */
  public static Integer WeekdayFromDate(LocalDate date, String startOfWeek) {
    if (date == null) {
      return null;
    }
    int daysInWeek = startOfWeek == null ? date.getDayOfWeek().getValue()
                       : date.with(TemporalAdjusters.previousOrSame(
                         DayOfWeek.valueOf(startOfWeek.toUpperCase()))).getDayOfWeek().getValue();
    return 7 - daysInWeek + 1;
  }

  /**
   * @return Returns the day number of the week from the given date.
   * If date is null, null will be returned.
   */
  public static Integer WeekdayFromDate(LocalDate date) {
    return WeekdayFromDate(date, "Sunday");
  }


  /**
   * @return Returns the day number in the year from the given date.
   * If date is null, null will be returned.
   */
  public static Integer YeardayFromDate(LocalDate date) {
    return date == null ? null : date.getDayOfYear();
  }


  /**
   * @return Returns the year from the given date.
   * If date is null, null will be returned
   */
  public static Integer YearFromDate(LocalDate date) {
    return date == null ? null : date.getYear();
  }

  /**
   * @return Returns the week number in the year from the given date.
   * If date is null, null will be returned.
   */
  public static Integer YearweekFromDate(LocalDate date) {
    // Start of the week is MONDAY and it should have minimum 7 days of the week.
    return date == null ? null :
             date.get(WeekFields.of(DayOfWeek.MONDAY, 7).weekOfWeekBasedYear());
  }
}
