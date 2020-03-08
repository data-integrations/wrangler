package io.cdap.functions;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertEquals(DateTime.parse("2008-08-05", DateTimeFormat.forPattern("yyyy-MM-dd")),
                        rows.get(0).getValue("DateFromDaysSince1"));
    Assert.assertEquals(DateTime.parse("1958-08-17", DateTimeFormat.forPattern("yyyy-MM-dd")),
                        rows.get(0).getValue("DateFromDaysSince2"));
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
    Assert.assertEquals(DateTime.parse("2010-12-02", DateTimeFormat.forPattern("yyyy-MM-dd")),
                        rows.get(0).getValue("DateFromComponents1"));
    Assert.assertEquals(DateTime.parse("1958-08-18", DateTimeFormat.forPattern("yyyy-MM-dd")),
                        rows.get(0).getValue("DateFromComponents2"));
    Assert.assertEquals(DateTime.parse("2020-03-07", DateTimeFormat.forPattern("yyyy-MM-dd")),
                        rows.get(0).getValue("DateFromComponents3"));
  }

  @Test
  public void testDateFromJulianDay() throws Exception {
    String[] directives = new String[]{
      "set-column DateFromJulianDay datetime:DateFromJulianDay(2454614)", // 2008–05–27
    };

    List<Row> rows = Arrays.asList(new Row());
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    DateTime result = (DateTime) rows.get(0).getValue("DateFromJulianDay");
    Assert.assertEquals(2008, result.getYear());
    Assert.assertEquals(5, result.getMonthOfYear());
    Assert.assertEquals(27, result.getDayOfMonth());
    Assert.assertEquals(5, result.getHourOfDay());
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
    Assert.assertEquals(DateTime.parse("2013-08-18", DateTimeFormat.forPattern("yyyy-MM-dd")),
                        rows.get(0).getValue("DateOffsetByComponents1"));
    Assert.assertEquals(DateTime.parse("2013-09-17", DateTimeFormat.forPattern("yyyy-MM-dd")),
                        rows.get(0).getValue("DateOffsetByComponents2"));
    Assert.assertEquals(DateTime.parse("2014-01-17", DateTimeFormat.forPattern("yyyy-MM-dd")),
                        rows.get(0).getValue("DateOffsetByComponents3"));
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
      "a",DateTime.parse("2008-08-18", DateTimeFormat.forPattern("yyyy-MM-dd"))).add(
      "b",DateTime.parse("1958-08-18", DateTimeFormat.forPattern("yyyy-MM-dd")
    )));

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(-18263, rows.get(0).getValue("DaysSinceFromDate1"));
    Assert.assertEquals(18263, rows.get(0).getValue("DaysSinceFromDate2"));
    Assert.assertEquals(-18263, rows.get(0).getValue("DaysSinceFromDate3"));
    Assert.assertEquals(-18263, rows.get(0).getValue("DaysSinceFromDate4"));
    Assert.assertEquals(-18263, rows.get(0).getValue("DaysSinceFromDate5"));
    Assert.assertEquals(18263, rows.get(0).getValue("DaysSinceFromDate6"));
  }

  @Test
  public void testDaysInMonth() throws Exception {
    String[] directives = new String[]{
      "set-column DaysInMonth1 datetime:DaysInMonth('1958-08-18')", //31
      "set-column DaysInMonth2 datetime:DaysInMonth(a)", //31
    };

    List<Row> rows = Arrays.asList(new Row(
      "a",DateTime.parse("1958-08-18", DateTimeFormat.forPattern("yyyy-MM-dd"))));

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(31, rows.get(0).getValue("DaysInMonth1"));
    Assert.assertEquals(31, rows.get(0).getValue("DaysInMonth2"));
  }

}
