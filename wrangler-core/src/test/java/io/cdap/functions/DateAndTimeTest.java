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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

/**
 * A class to test {@link DateAndTime} functions.
 */
public class DateAndTimeTest {

  @Test
  public void testCurrentDateTime() throws Exception {
    String[] directives = new String[]{
      "set-column CurrentDate datetime:CurrentDate()",
      "set-column CurrentTime datetime:CurrentTime()",
      "set-column CurrentTimeMS datetime:CurrentTimeMS()",
      "set-column CurrentDateTime datetime:CurrentDateTime()",
      "set-column CurrentTimestampMS datetime:CurrentTimestampMS()",
      "set-column CurrentTimestampMS datetime:CurrentTimestampMS()",
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertNotNull(rows.get(0).getValue("CurrentDate"));
    Assert.assertNotNull(rows.get(0).getValue("CurrentTime"));
    Assert.assertNotNull(rows.get(0).getValue("CurrentTimeMS"));
    Assert.assertNotNull(rows.get(0).getValue("CurrentDateTime"));
    Assert.assertNotNull(rows.get(0).getValue("CurrentTimestampMS"));
    Assert.assertNotNull(rows.get(0).getValue("CurrentTimestampMS"));
  }

  @Test
  public void testDateFromDaysSince() throws Exception {
    String[] directives = new String[]{
      "set-column DateFromDaysSince1 datetime:DateFromDaysSince(18250, datetime:GetDate('1958-08-18'))", // 2008-08-05
      "set-column DateFromDaysSince2 datetime:DateFromDaysSince(-1, datetime:GetDate('1958-08-18'))", // 1958-08-17
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDate.of(2008, 8, 5), rows.get(0).getValue("DateFromDaysSince1"));
    Assert.assertEquals(LocalDate.of(1958, 8, 17), rows.get(0).getValue("DateFromDaysSince2"));
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
    Assert.assertEquals(LocalDate.of(2010, 12, 2), rows.get(0).getValue("DateFromComponents1"));
    Assert.assertEquals(LocalDate.of(1958, 8, 18), rows.get(0).getValue("DateFromComponents2"));
    Assert.assertEquals(LocalDate.of(2020, 3, 7), rows.get(0).getValue("DateFromComponents3"));
  }

  @Test
  public void testDateFromJulianDay() throws Exception {
    String[] directives = new String[]{
      "set-column DateFromJulianDay datetime:DateFromJulianDay(2454614L)", // 2008–05–27
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
      "set-column DateOffsetByComponents1 datetime:DateOffsetByComponents(datetime:GetDate('2011-08-18'),2,0,0)", // 2013-08-18
      "set-column DateOffsetByComponents2 datetime:DateOffsetByComponents(datetime:GetDate('2011-08-18'),2,1,-1)", // 2013-09-17
      "set-column DateOffsetByComponents3 datetime:DateOffsetByComponents(datetime:GetDate('2011-08-18'),2,5,-1)", // 2014-01-17
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDate.of(2013, 8, 18), rows.get(0).getValue("DateOffsetByComponents1"));
    Assert.assertEquals(LocalDate.of(2013, 9, 17), rows.get(0).getValue("DateOffsetByComponents2"));
    Assert.assertEquals(LocalDate.of(2014, 1, 17), rows.get(0).getValue("DateOffsetByComponents3"));
  }

  @Test
  public void testDaysSinceFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column DaysSinceFromDate1 datetime:DaysSinceFromDate(datetime:GetDate('2008-08-18')," +
        "datetime:GetDate('1958-08-18'))", //-18263
      "set-column DaysSinceFromDate2 datetime:DaysSinceFromDate(datetime:GetDate('1958-08-18')," +
        "datetime:GetDate('2008-08-18'))", //18263
      "set-column DaysSinceFromDate3 datetime:DaysSinceFromDate(a,datetime:GetDate('1958-08-18'))", //-18263
      "set-column DaysSinceFromDate4 datetime:DaysSinceFromDate(datetime:GetDate('2008-08-18'),b)", //-18263
      "set-column DaysSinceFromDate5 datetime:DaysSinceFromDate(a,b)", //-18263
      "set-column DaysSinceFromDate6 datetime:DaysSinceFromDate(b,a)", //18263
    };

    List<Row> rows = Arrays.asList(new Row(
      "a", LocalDate.of(2008, 8, 18)).add(
      "b", LocalDate.of(1958, 8, 18))
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
      "set-column DaysInMonth1 datetime:DaysInMonth(datetime:GetDate('1958-08-18'))", //31
      "set-column DaysInMonth2 datetime:DaysInMonth(a)", //31
    };

    List<Row> rows = Arrays.asList(new Row(
      "a", LocalDate.of(1958, 8, 18))
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(31, rows.get(0).getValue("DaysInMonth1"));
    Assert.assertEquals(31, rows.get(0).getValue("DaysInMonth2"));
  }

  @Test
  public void testDaysInYear() throws Exception {
    String[] directives = new String[]{
      "set-column DaysInYear1 datetime:DaysInYear(datetime:GetDate('2012-08-18'))", //366
      "set-column DaysInYear2 datetime:DaysInYear(a)", //366
      "set-column DaysInYear3 datetime:DaysInYear(datetime:GetDate('2011-08-18'))", //365
      "set-column DaysInYear4 datetime:DaysInYear(b)", //365
    };

    List<Row> rows = Arrays.asList(new Row(
      "a", LocalDate.of(2012, 8, 18)).add("b", LocalDate.of(2011, 8, 18))
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
      "set-column DateOffsetByDays1 datetime:DateOffsetByDays(datetime:GetDate('2011-08-18'), 2)", // 2011-8-20
      "set-column DateOffsetByDays2 datetime:DateOffsetByDays(datetime:GetDate('2011-08-18'), -31)",// 2011-7-18
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDate.of(2011, 8, 20), rows.get(0).getValue("DateOffsetByDays1"));
    Assert.assertEquals(LocalDate.of(2011, 7, 18), rows.get(0).getValue("DateOffsetByDays2"));
  }

  @Test
  public void testHoursFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column HoursFromTime1 datetime:HoursFromTime(datetime:GetTime('22:30:00'))", //22
      "set-column HoursFromTime2 datetime:HoursFromTime(datetime:GetTime('22:30:00.4'))", //22
      "set-column HoursFromTime3 datetime:HoursFromTime(datetime:GetTime('22:30:00.43'))", //22
      "set-column HoursFromTime4 datetime:HoursFromTime(datetime:GetTime('22:30:00.434'))", //22
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
      "set-column JulianDayFromDate datetime:JulianDayFromDate(datetime:GetDate('2008-05-27'))", // 2454614
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(2454614L, rows.get(0).getValue("JulianDayFromDate"));
  }

  @Test
  public void testNanoSecondsFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column NanoSecondsFromTime datetime:NanoSecondsFromTime(datetime:GetTime('22:30:00.32'))", // 2454614
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(320000000, rows.get(0).getValue("NanoSecondsFromTime"));
  }

  @Test
  public void testMicroSecondsFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column MicroSecondsFromTime datetime:MicroSecondsFromTime(datetime:GetTime('22:30:00.32'))", // 2454614
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(320000, rows.get(0).getValue("MicroSecondsFromTime"));
  }

  @Test
  public void testMilliSecondsFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column MilliSecondsFromTime datetime:MilliSecondsFromTime(datetime:GetTime('22:30:00.32'))", // 2454614
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(320, rows.get(0).getValue("MilliSecondsFromTime"));
  }

  @Test
  public void testMidnightSecondsFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column MidnightSecondsFromTime datetime:MidnightSecondsFromTime(datetime:GetTime('00:30:52'))", // 1852
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(1852, rows.get(0).getValue("MidnightSecondsFromTime"));
  }

  @Test
  public void testMinutesFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column MinutesFromTime datetime:MinutesFromTime(datetime:GetTime('22:30:52'))", // 30
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(30, rows.get(0).getValue("MinutesFromTime"));
  }

  @Test
  public void testMonthDayFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column MonthDayFromDate datetime:MonthDayFromDate(datetime:GetDate('2008-08-18'))", // 18
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(18, rows.get(0).getValue("MonthDayFromDate"));
  }

  @Test
  public void testMonthFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column MonthFromDate datetime:MonthFromDate(datetime:GetDate('2008-08-18'))", // 8
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(8, rows.get(0).getValue("MonthFromDate"));
  }

  @Test
  public void testNextWeekdayFromDate() throws Exception {
    String[] directives = new String[]{
      // 2008-08-21
      "set-column NextWeekdayFromDate1 datetime:NextWeekdayFromDate(datetime:GetDate('2008-08-18'), 'Thursday')",
      "set-column NextWeekdayFromDate2 datetime:NextWeekdayFromDate(datetime:GetDate('2008-08-18'), 'Thu')",
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDate.of(2008, 8, 21), rows.get(0).getValue("NextWeekdayFromDate1"));
    Assert.assertEquals(LocalDate.of(2008, 8, 21), rows.get(0).getValue("NextWeekdayFromDate2"));
  }

  @Test
  public void testNthWeekdayFromDate() throws Exception {
    String[] directives = new String[]{
      // 2009-08-20
      "set-column NthWeekdayFromDate1 datetime:NthWeekdayFromDate(datetime:GetDate('2009-08-18'), 'Thursday', 1)",
      "set-column NthWeekdayFromDate2 datetime:NthWeekdayFromDate(datetime:GetDate('2009-08-18'), 'Thu', 1)",
      // 2009-08-06
      "set-column NthWeekdayFromDate3 datetime:NthWeekdayFromDate(datetime:GetDate('2009-08-18'), 'Thursday', -2)",
      "set-column NthWeekdayFromDate4 datetime:NthWeekdayFromDate(datetime:GetDate('2009-08-18'), 'Thu', -2)",
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDate.of(2009, 8, 20), rows.get(0).getValue("NthWeekdayFromDate1"));
    Assert.assertEquals(LocalDate.of(2009, 8, 20), rows.get(0).getValue("NthWeekdayFromDate2"));
    Assert.assertEquals(LocalDate.of(2009, 8, 6), rows.get(0).getValue("NthWeekdayFromDate3"));
    Assert.assertEquals(LocalDate.of(2009, 8, 6), rows.get(0).getValue("NthWeekdayFromDate4"));
  }

  @Test
  public void testPreviousWeekdayFromDate() throws Exception {
    String[] directives = new String[]{
      // 2008-08-14
      "set-column PreviousWeekdayFromDate1 datetime:PreviousWeekdayFromDate(datetime:GetDate('2008-08-18'), " +
        "'Thursday')",
      "set-column PreviousWeekdayFromDate2 datetime:PreviousWeekdayFromDate(datetime:GetDate('2008-08-18'), 'Thu')",
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDate.of(2008, 8, 14), rows.get(0).getValue("PreviousWeekdayFromDate1"));
    Assert.assertEquals(LocalDate.of(2008, 8, 14), rows.get(0).getValue("PreviousWeekdayFromDate2"));
  }

  @Test
  public void testSecondsFromTime() throws Exception {
    String[] directives = new String[]{
      "set-column SecondsFromTime datetime:SecondsFromTime(datetime:GetTime('22:30:52'))", // 52
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
        "datetime:SecondsSinceFromDateTime(datetime:GetDateTime('2008-08-18 22:30:52'), " +
        "datetime:GetDateTime('2008-08-19 22:30:52'))", // -86400
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
    Assert.assertEquals(LocalTime.of(10, 12, 2, 0), rows.get(0).getValue("TimeFromComponents"));
  }

  @Test
  public void testTimeFromMidnightSeconds() throws Exception {
    String[] directives = new String[]{
      "set-column TimeFromMidnightSeconds1 datetime:TimeFromMidnightSeconds(240)", // 00:04:00
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalTime.of(0, 4, 0), rows.get(0).getValue("TimeFromMidnightSeconds1"));
  }

  @Test
  public void testTimestampFromDateTime() throws Exception {
    String[] directives = new String[]{
      "set-column GetDateTime datetime:GetDateTime(datetime:GetDate('2008-08-18'), " +
        "datetime:GetTime('22:30:52'))"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDateTime.of(2008, 8, 18, 22, 30, 52), rows.get(0).getValue("GetDateTime"));
  }

  @Test
  public void testTimestampFromSecondSince() throws Exception {
    String[] directives = new String[]{
      "set-column TimestampFromSecondSince datetime:DateTimeFromSecondsSince(2563, " +
        "datetime:GetDateTime('2008-08-18 22:30:52'))"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDateTime.of(2008, 8, 18, 23, 13, 35), rows.get(0).getValue("TimestampFromSecondSince"));
  }

  @Test
  public void testTimestampFromTimet() throws Exception {
    String[] directives = new String[]{
      "set-column TimestampFromEpoch1 datetime:DateTimeFromEpoch(1234567890L)",
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDateTime.of(2009, 2, 13, 23, 31, 30), rows.get(0).getValue("TimestampFromEpoch1"));
  }

  @Test
  public void testTimestampFromTime2() throws Exception {
    String[] directives = new String[]{
      "set-column DateTimeFromTime datetime:DateTimeFromTime(datetime:GetTime('12:03:22')," +
        " datetime:GetDateTime('2008-08-18 22:30:52'))"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDateTime.of(2008, 8, 18, 12, 3, 22), rows.get(0).getValue("DateTimeFromTime"));
  }

  @Test
  public void testTimestampOffsetByComponents() throws Exception {
    String[] directives = new String[]{
      "set-column a datetime:DateTimeOffsetByComponents(datetime:GetDateTime('2009-08-18 14:05:29'), " +
        "0, 2, -4, 2, 0, 20)"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDateTime.of(2009, 10, 14, 16, 5, 49), rows.get(0).getValue("a"));
  }

  @Test
  public void testTimestampOffsetBySeconds() throws Exception {
    String[] directives = new String[]{
      "set-column a datetime:DateTimeOffsetBySeconds(datetime:GetDateTime('2009-08-18 14:05:29'), 32760)"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(LocalDateTime.of(2009, 8, 18, 23, 11, 29), rows.get(0).getValue("a"));
  }

  @Test
  public void testEpochFromTimestamp() throws Exception {
    String[] directives = new String[]{
      "set-column epoch datetime:EpochFromDateTime(datetime:GetDateTime('2009-02-13 23:31:30'))"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(1234567890L, rows.get(0).getValue("epoch"));
  }

  @Test
  public void testWeekdayFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column weekday1 datetime:WeekdayFromDate(datetime:GetDate('2008-08-18'))",
      "set-column weekday2 datetime:WeekdayFromDate(datetime:GetDate('2008-08-18'), 'saturday')",
      "set-column weekday3 datetime:WeekdayFromDate(datetime:GetDate('2008-08-18'), 'friday')",
      "set-column weekday4 datetime:WeekdayFromDate(datetime:GetDate('2008-08-18'), 'thursday')",
      "set-column weekday5 datetime:WeekdayFromDate(datetime:GetDate('2008-08-18'), 'wednesday')",
      "set-column weekday6 datetime:WeekdayFromDate(datetime:GetDate('2008-08-18'), 'tuesday')",
      "set-column weekday7 datetime:WeekdayFromDate(datetime:GetDate('2008-08-18'), 'monday')",
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
      "set-column year datetime:YeardayFromDate(datetime:GetDate('2008-08-18'))"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(231, rows.get(0).getValue("year"));
  }

  @Test
  public void testYearweekFromDate() throws Exception {
    String[] directives = new String[]{
      "set-column yearweek datetime:YearweekFromDate(datetime:GetDate('2008-08-18'))"
    };
    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(33, rows.get(0).getValue("yearweek"));
  }
}
