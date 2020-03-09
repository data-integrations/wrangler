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

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;

/**
 * A class to test {@link DateAndTime} functions.
 */
public class DateAndTimeTest {
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

  private static final DateTimeFormatter DAY_OF_THE_WEEK =
    new DateTimeFormatterBuilder()
      .appendValue(ChronoField.DAY_OF_WEEK)
      .toFormatter();

  private static LocalDate getDate(String date) {
    return LocalDate.parse(date, DATE_TIME_FORMAT);
  }

  private static LocalDateTime getTimestamp(String date) {
    return LocalDateTime.parse(date, DATE_TIME_FORMAT);
  }

  private static LocalTime getTime(String time) {
    return LocalTime.parse(time, TIME_FORMAT);
  }

  @Test
  public void testTimestamp() throws Exception {
    Instant now = Instant.now();
    Assert.assertEquals(1,1);
  }

  @Test
  public void testCurrentDateTime() throws Exception {
    String[] directives = new String[]{
      "set-column CurrentDate datetime:CurrentDate()",
      "set-column CurrentTime datetime:CurrentTime()",
      "set-column CurrentTimeMS datetime:CurrentTimeMS()",
      "set-column CurrentTimestamp datetime:CurrentTimestamp()",
      "set-column CurrentTimestampMS datetime:CurrentTimestampMS()",
      "set-column CurrentTimestampMS datetime:CurrentTimestampMS()",
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertNotNull(rows.get(0).getValue("CurrentDate"));
    Assert.assertNotNull(rows.get(0).getValue("CurrentTime"));
    Assert.assertNotNull(rows.get(0).getValue("CurrentTimeMS"));
    Assert.assertNotNull(rows.get(0).getValue("CurrentTimestamp"));
    Assert.assertNotNull(rows.get(0).getValue("CurrentTimestampMS"));
    Assert.assertNotNull(rows.get(0).getValue("CurrentTimestampMS"));
  }

  @Test
  public void testDateFromDaysSince() throws Exception {
    String[] directives = new String[]{
      "set-column DateFromDaysSince1 datetime:DateFromDaysSince(18250, '1958-08-18')", // 2008-08-05
      "set-column DateFromDaysSince2 datetime:DateFromDaysSince(-1, '1958-08-18')", // 1958-08-17
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getDate("2008-08-05"), rows.get(0).getValue("DateFromDaysSince1"));
    Assert.assertEquals(getDate("1958-08-17"), rows.get(0).getValue("DateFromDaysSince2"));
  }

  @Test
  public void testDateFromComponents() throws Exception {
    String[] directives = new String[]{
      "set-column DateFromComponents1 datetime:DateFromComponents(2010, 12, 2)", // 2010-12-02
      "set-column DateFromComponents2 datetime:DateFromComponents(1958, 8, 18)", // 1958-08-17
      "set-column DateFromComponents3 datetime:DateFromComponents(2020, 3, 7)" // 2020-03-07
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getDate("2010-12-02"), rows.get(0).getValue("DateFromComponents1"));
    Assert.assertEquals(getDate("1958-08-18"), rows.get(0).getValue("DateFromComponents2"));
    Assert.assertEquals(getDate("2020-03-07"), rows.get(0).getValue("DateFromComponents3"));
  }

  @Test
  public void testDateFromJulianDay() throws Exception {
    String[] directives = new String[]{
      "set-column DateFromJulianDay datetime:DateFromJulianDay(2454614)", // 2008–05–27
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    LocalDate result = (LocalDate) rows.get(0).getValue("DateFromJulianDay");
    Assert.assertEquals(2008, result.getYear());
    Assert.assertEquals(5, result.getMonthValue());
    Assert.assertEquals(27, result.getDayOfMonth());
  }

  @Test
  public void testDateOffsetByComponents() throws Exception {
    String[] directives = new String[]{
      "set-column DateOffsetByComponents1 datetime:DateOffsetByComponents('2011-08-18',2,0,0)", // 2013-08-18
      "set-column DateOffsetByComponents2 datetime:DateOffsetByComponents('2011-08-18',2,1,-1)", // 2013-09-17
      "set-column DateOffsetByComponents3 datetime:DateOffsetByComponents('2011-08-18',2,5,-1)", // 2014-01-17
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getDate("2013-08-18"), rows.get(0).getValue("DateOffsetByComponents1"));
    Assert.assertEquals(getDate("2013-09-17"), rows.get(0).getValue("DateOffsetByComponents2"));
    Assert.assertEquals(getDate("2014-01-17"), rows.get(0).getValue("DateOffsetByComponents3"));
  }

  @Test
  public void testDaysSinceFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column DaysSinceFromDate1 datetime:DaysSinceFromDate('2008-08-18','1958-08-18')", //-18263
      "set-column DaysSinceFromDate2 datetime:DaysSinceFromDate('1958-08-18','2008-08-18')", //18263
      "set-column DaysSinceFromDate3 datetime:DaysSinceFromDate(a,'1958-08-18')", //-18263
      "set-column DaysSinceFromDate4 datetime:DaysSinceFromDate('2008-08-18',b)", //-18263
      "set-column DaysSinceFromDate5 datetime:DaysSinceFromDate(a,b)", //-18263
      "set-column DaysSinceFromDate6 datetime:DaysSinceFromDate(b,a)", //18263
    };

    List<Row> rows = Arrays.asList(new Row(
      "a", getDate("2008-08-18")).add(
      "b", getDate("1958-08-18"))
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(-18263L, rows.get(0).getValue("DaysSinceFromDate1"));
    Assert.assertEquals(18263L, rows.get(0).getValue("DaysSinceFromDate2"));
    Assert.assertEquals(-18263L, rows.get(0).getValue("DaysSinceFromDate3"));
    Assert.assertEquals(-18263L, rows.get(0).getValue("DaysSinceFromDate4"));
    Assert.assertEquals(-18263L, rows.get(0).getValue("DaysSinceFromDate5"));
    Assert.assertEquals(18263L, rows.get(0).getValue("DaysSinceFromDate6"));
  }

  @Test
  public void testDaysInMonth() throws Exception {
    String[] directives = new String[]{
      "set-column DaysInMonth1 datetime:DaysInMonth('1958-08-18')", //31
      "set-column DaysInMonth2 datetime:DaysInMonth(a)", //31
    };

    List<Row> rows = Arrays.asList(new Row(
      "a", getDate("1958-08-18"))
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(31, rows.get(0).getValue("DaysInMonth1"));
    Assert.assertEquals(31, rows.get(0).getValue("DaysInMonth2"));
  }

  @Test
  public void testDaysInYear() throws Exception {
    String[] directives = new String[]{
      "set-column DaysInYear1 datetime:DaysInYear('2012-08-18')", //366
      "set-column DaysInYear2 datetime:DaysInYear(a)", //366
      "set-column DaysInYear3 datetime:DaysInYear('2011-08-18')", //365
      "set-column DaysInYear4 datetime:DaysInYear(b)", //365
    };

    List<Row> rows = Arrays.asList(new Row(
      "a", getDate("2012-08-18"))
      .add("b", getDate("2011-08-18"))
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(366, rows.get(0).getValue("DaysInYear1"));
    Assert.assertEquals(366, rows.get(0).getValue("DaysInYear2"));
    Assert.assertEquals(365, rows.get(0).getValue("DaysInYear3"));
    Assert.assertEquals(365, rows.get(0).getValue("DaysInYear4"));
  }

  @Test
  public void testDateOffsetByDays() throws Exception {
    String[] directives = new String[]{
      "set-column DateOffsetByDays1 datetime:DateOffsetByDays('2011-08-18', 2)", // 2011-8-20
      "set-column DateOffsetByDays2 datetime:DateOffsetByDays('2011-08-18', -31)",// 2011-7-18
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getDate("2011-08-20"), rows.get(0).getValue("DateOffsetByDays1"));
    Assert.assertEquals(getDate("2011-07-18"), rows.get(0).getValue("DateOffsetByDays2"));
  }

  @Test
  public void testHoursFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column HoursFromTime1 datetime:HoursFromTime('22:30:00')", //22
      "set-column HoursFromTime2 datetime:HoursFromTime('22:30:00.4')", //22
      "set-column HoursFromTime3 datetime:HoursFromTime('22:30:00.43')", //22
      "set-column HoursFromTime4 datetime:HoursFromTime('22:30:00.434')", //22
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(22, rows.get(0).getValue("HoursFromTime1"));
    Assert.assertEquals(22, rows.get(0).getValue("HoursFromTime2"));
    Assert.assertEquals(22, rows.get(0).getValue("HoursFromTime3"));
    Assert.assertEquals(22, rows.get(0).getValue("HoursFromTime4"));
  }

  @Test
  public void testJulianDayFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column JulianDayFromDate datetime:JulianDayFromDate('2008-05-27')", // 2454614
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(2454614L, rows.get(0).getValue("JulianDayFromDate"));
  }

  @Test
  public void testNanoSecondsFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column NanoSecondsFromTime datetime:NanoSecondsFromTime('22:30:00.32')", // 2454614
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(320000000, rows.get(0).getValue("NanoSecondsFromTime"));
  }

  @Test
  public void testMicroSecondsFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column MicroSecondsFromTime datetime:MicroSecondsFromTime('22:30:00.32')", // 2454614
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(320000, rows.get(0).getValue("MicroSecondsFromTime"));
  }

  @Test
  public void testMilliSecondsFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column MilliSecondsFromTime datetime:MilliSecondsFromTime('22:30:00.32')", // 2454614
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(320, rows.get(0).getValue("MilliSecondsFromTime"));
  }

  @Test
  public void testMidnightSecondsFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column MidnightSecondsFromTime datetime:MidnightSecondsFromTime('00:30:52')", // 1852
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(1852, rows.get(0).getValue("MidnightSecondsFromTime"));
  }

  @Test
  public void testMinutesFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column MinutesFromTime datetime:MinutesFromTime('22:30:52')", // 30
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(30, rows.get(0).getValue("MinutesFromTime"));
  }

  @Test
  public void testMonthDayFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column MonthDayFromDate datetime:MonthDayFromDate('2008-08-18')", // 18
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(18, rows.get(0).getValue("MonthDayFromDate"));
  }

  @Test
  public void testMonthFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column MonthFromDate datetime:MonthFromDate('2008-08-18')", // 8
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(8, rows.get(0).getValue("MonthFromDate"));
  }

  @Test
  public void testNextWeekdayFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column NextWeekdayFromDate1 datetime:NextWeekdayFromDate('2008-08-18', 'Thursday')", // 2008-08-21
      "set-column NextWeekdayFromDate2 datetime:NextWeekdayFromDate('2008-08-18', 'Thu')", // 2008-08-21
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getDate("2008-08-21"), rows.get(0).getValue("NextWeekdayFromDate1"));
    Assert.assertEquals(getDate("2008-08-21"), rows.get(0).getValue("NextWeekdayFromDate2"));
  }

  @Test
  public void testNthWeekdayFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column NthWeekdayFromDate1 datetime:NthWeekdayFromDate('2009-08-18', 'Thursday', 1)", // 2009-08-20
      "set-column NthWeekdayFromDate2 datetime:NthWeekdayFromDate('2009-08-18', 'Thu', 1)", // 2009-08-20
      "set-column NthWeekdayFromDate3 datetime:NthWeekdayFromDate('2009-08-18', 'Thursday', -2)", // 2009-08-06
      "set-column NthWeekdayFromDate4 datetime:NthWeekdayFromDate('2009-08-18', 'Thu', -2)", // 2009-08-06
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getDate("2009-08-20"), rows.get(0).getValue("NthWeekdayFromDate1"));
    Assert.assertEquals(getDate("2009-08-20"), rows.get(0).getValue("NthWeekdayFromDate2"));
    Assert.assertEquals(getDate("2009-08-06"), rows.get(0).getValue("NthWeekdayFromDate3"));
    Assert.assertEquals(getDate("2009-08-06"), rows.get(0).getValue("NthWeekdayFromDate4"));
  }

  @Test
  public void testPreviousWeekdayFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column PreviousWeekdayFromDate1 datetime:PreviousWeekdayFromDate('2008-08-18', 'Thursday')", // 2008-08-14
      "set-column PreviousWeekdayFromDate2 datetime:PreviousWeekdayFromDate('2008-08-18', 'Thu')", // 2008-08-14
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getDate("2008-08-14"), rows.get(0).getValue("PreviousWeekdayFromDate1"));
    Assert.assertEquals(getDate("2008-08-14"), rows.get(0).getValue("PreviousWeekdayFromDate2"));
  }

  @Test
  public void testSecondsFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column SecondsFromTime datetime:SecondsFromTime('22:30:52')", // 52
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(52, rows.get(0).getValue("SecondsFromTime"));
  }

  @Test
  public void testSecondsSinceFromTimestamp() throws Exception {
    String[] directives = new String[]{
      "set-column SecondsSinceFromTimestamp1 " +
        "datetime:SecondsSinceFromTimestamp('2008-08-18 22:30:52', '2008-08-19 22:30:52')", // -86400
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(-86400L, rows.get(0).getValue("SecondsSinceFromTimestamp1"));
  }

  @Test
  public void testTimeFromComponents() throws Exception {
    String[] directives = new String[]{
      "set-column TimeFromComponents datetime:TimeFromComponents(10, 12, 2, 0)", // 10:12:02.0
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getTime("10:12:02.0"), rows.get(0).getValue("TimeFromComponents"));
  }

  @Test
  public void testTimeFromMidnightSeconds() throws Exception {
    String[] directives = new String[]{
      "set-column TimeFromMidnightSeconds1 datetime:TimeFromMidnightSeconds(240)", // 00:04:00
      "set-column TimeFromMidnightSeconds2 datetime:TimeFromMidnightSeconds('240')", // 00:04:00
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getTime("00:04:00"), rows.get(0).getValue("TimeFromMidnightSeconds1"));
    Assert.assertEquals(getTime("00:04:00"), rows.get(0).getValue("TimeFromMidnightSeconds2"));
  }

  @Test
  public void testTimestampFromDateTime() throws Exception {
    String[] directives = new String[]{
      "set-column TimestampFromDateTime datetime:TimestampFromDateTime('2008-08-18', '22:30:52')"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getTimestamp("2008-08-18 22:30:52"), rows.get(0).getValue("TimestampFromDateTime"));
  }

  @Test
  public void testTimestampFromSecondSince() throws Exception {
    String[] directives = new String[]{
      "set-column TimestampFromSecondSince datetime:TimestampFromSecondSince(2563, '2008-08-18 22:30:52')"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getTimestamp("2008-08-18 23:13:35"), rows.get(0).getValue("TimestampFromSecondSince"));
  }

  @Test
  public void testTimestampFromTimet() throws Exception {
    String[] directives = new String[]{
      "set-column TimestampFromEpoch1 datetime:TimestampFromEpoch(1234567890)",
      "set-column TimestampFromEpoch2 datetime:TimestampFromEpoch('1234567890')"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getTimestamp("2009-02-13 23:31:30"), rows.get(0).getValue("TimestampFromEpoch1"));
    Assert.assertEquals(getTimestamp("2009-02-13 23:31:30"), rows.get(0).getValue("TimestampFromEpoch2"));
  }

  @Test
  public void testTimestampFromTime2() throws Exception {
    String[] directives = new String[]{
      "set-column TimestampFromTime datetime:TimestampFromTime('12:03:22', '2008-08-18 22:30:52')"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getTimestamp("2008-08-18 12:03:22"), rows.get(0).getValue("TimestampFromTime"));
  }

  @Test
  public void testTimestampOffsetByComponents() throws Exception {
    String[] directives = new String[]{
      "set-column a datetime:TimestampOffsetByComponents('2009-08-18 14:05:29', 0, 2, -4, 2, 0, 20)"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getTimestamp("2009-10-14 16:05:49"), rows.get(0).getValue("a"));
  }

  @Test
  public void testTimestampOffsetBySeconds() throws Exception {
    String[] directives = new String[]{
      "set-column a datetime:TimestampOffsetBySeconds('2009-08-18 14:05:29', 32760)"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(getTimestamp("2009-08-18 23:11:29"), rows.get(0).getValue("a"));
  }

  @Test
  public void testEpochFromTimestamp() throws Exception {
    String[] directives = new String[]{
      "set-column epoch datetime:EpochFromTimestamp('2009-02-13 23:31:30')"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(1234567890L, rows.get(0).getValue("epoch"));
  }

  @Test
  public void testWeekdayFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column weekday1 datetime:WeekdayFromDate('2008-08-18')",
      "set-column weekday2 datetime:WeekdayFromDate('2008-08-18', 'saturday')",
      "set-column weekday3 datetime:WeekdayFromDate('2008-08-18', 'friday')",
      "set-column weekday4 datetime:WeekdayFromDate('2008-08-18', 'thursday')",
      "set-column weekday5 datetime:WeekdayFromDate('2008-08-18', 'wednesday')",
      "set-column weekday6 datetime:WeekdayFromDate('2008-08-18', 'tuesday')",
      "set-column weekday7 datetime:WeekdayFromDate('2008-08-18', 'monday')",
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(1, rows.get(0).getValue("weekday1"));
    Assert.assertEquals(2, rows.get(0).getValue("weekday2"));
    Assert.assertEquals(3, rows.get(0).getValue("weekday3"));
    Assert.assertEquals(4, rows.get(0).getValue("weekday4"));
    Assert.assertEquals(5, rows.get(0).getValue("weekday5"));
    Assert.assertEquals(6, rows.get(0).getValue("weekday6"));
    Assert.assertEquals(7, rows.get(0).getValue("weekday7"));
  }

  @Test
  public void testYeardayFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column year datetime:YeardayFromDate('2008-08-18')"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(231, rows.get(0).getValue("year"));
  }

  @Test
  public void testYearweekFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column yearweek datetime:YearweekFromDate('2008-08-18')"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(33, rows.get(0).getValue("yearweek"));
  }
}
