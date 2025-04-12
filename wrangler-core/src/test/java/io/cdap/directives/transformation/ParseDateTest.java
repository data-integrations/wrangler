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

package io.cdap.directives.transformation;

import io.cdap.directives.parser.ParseDate;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link ParseDate}
 */
public class ParseDateTest {

  @Before
  public void setUp() {
    // Force UTC timezone for consistency across environments
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @Test
  public void testSimpleDateParserAndDiff() throws Exception {
    String[] directives = new String[] {
        "parse-as-simple-date date1 MM/dd/yyyy HH:mm",
        "parse-as-simple-date date2 MM/dd/yyyy HH:mm",
        "diff-date date1 date2 difference"
    };

    Row row1 = new Row();
    row1.add("date1", "12/10/2016 07:45");
    row1.add("date2", "12/10/2016 06:45");

    Row row2 = new Row();
    row2.add("date1", "2/1/1990 12:01");
    row2.add("date2", "1/1/1990 12:00");

    Row row3 = new Row();
    row3.add("date1", "03/03/1998 2:02");
    row3.add("date2", "03/03/1998 2:02");

    List<Row> rows = TestingRig.execute(directives, Arrays.asList(row1, row2, row3));

    Assert.assertEquals(TimeUnit.HOURS.toMillis(1), rows.get(0).getValue("difference"));
    Assert.assertEquals(2678460000L, rows.get(1).getValue("difference"));
    Assert.assertEquals(0L, rows.get(2).getValue("difference"));
    Assert.assertEquals(3, rows.size());
  }

  @Test
  public void testSimpleDateWithPatterns() throws Exception {
    String[] directives = new String[] {
        "parse-as-simple-date date1 MM/dd/yyyy",
        "parse-as-simple-date date2 dd/MM/yyyy",
        "parse-as-simple-date date3 MM-dd-yyyy",
        "parse-as-simple-date date4 MM-dd-yy",
        "parse-as-simple-date date5 yyyy-MM-dd",
        "parse-as-simple-date date6 yyyy-MM-dd HH:mm:ss",
        "parse-as-simple-date date7 MM-dd-yyyy 'at' HH:mm:ss z",
        "parse-as-simple-date date8 dd/MM/yy HH:mm:ss",
        "parse-as-simple-date date9 yyyy,MM.dd'T'HH:mm:ss.SSSZ",
        "parse-as-simple-date date10 MM.dd.yyyy HH:mm:ss.SSS",
        "parse-as-simple-date date11 EEE, d MMM yyyy HH:mm:ss",
        "parse-as-simple-date date12 EEE, MMM d, ''yy",
        "parse-as-simple-date date13 h:mm a",
        "parse-as-simple-date date14 K:mm a, z",
        "parse-as-simple-date date15 yyyy.MM.dd G 'at' HH:mm:ss z"
    };

    Row row = new Row();
    row.add("date1", "12/10/2016");
    row.add("date2", "10/12/2016");
    row.add("date3", "12-10-2016");
    row.add("date4", "12-10-16");
    row.add("date5", "2016-12-10");
    row.add("date6", "2016-12-10 06:45:11");
    row.add("date7", "12-10-2016 at 06:45:11 PST");
    row.add("date8", "10/12/2016 06:45:11");
    row.add("date9", "2016,12.10T06:45:11.111-0800");
    row.add("date10", "12.10.2016 06:45:11.111");
    row.add("date11", "Sat, 10 Dec 2016 06:45:11");
    row.add("date12", "Sat, Dec 10, '16");
    row.add("date13", "06:45 PM");
    row.add("date14", "06:45 PM, PST");
    row.add("date15", "2016.12.10 AD at 06:45:11 PST");

    List<Row> rows = TestingRig.execute(directives, Arrays.asList(row));

    LocalDate localDate = LocalDate.of(2016, 12, 10);
    LocalTime localTime = LocalTime.of(6, 45, 11);
    ZonedDateTime baseUTC = ZonedDateTime.of(localDate, localTime, ZoneOffset.UTC);
    ZonedDateTime baseZeroTimeUTC = ZonedDateTime.of(localDate, LocalTime.MIDNIGHT, ZoneOffset.UTC);

    Assert.assertEquals(baseZeroTimeUTC, rows.get(0).getValue("date1"));
    Assert.assertEquals(baseZeroTimeUTC, rows.get(0).getValue("date2"));
    Assert.assertEquals(baseZeroTimeUTC, rows.get(0).getValue("date3"));
    Assert.assertEquals(baseZeroTimeUTC, rows.get(0).getValue("date4"));
    Assert.assertEquals(baseZeroTimeUTC, rows.get(0).getValue("date5"));
    Assert.assertEquals(baseUTC, rows.get(0).getValue("date6"));

    ZonedDateTime pstConverted = baseUTC.minusHours(8);
    Assert.assertEquals(pstConverted, rows.get(0).getValue("date7"));

    Assert.assertEquals(baseUTC, rows.get(0).getValue("date8"));

    Assert.assertEquals(pstConverted.plusNanos(TimeUnit.MILLISECONDS.toNanos(111)), rows.get(0).getValue("date9"));
    Assert.assertEquals(baseUTC.plusNanos(TimeUnit.MILLISECONDS.toNanos(111)), rows.get(0).getValue("date10"));
    Assert.assertEquals(baseUTC, rows.get(0).getValue("date11"));
    Assert.assertEquals(baseZeroTimeUTC, rows.get(0).getValue("date12"));

    ZonedDateTime timeOnlyUTC = ZonedDateTime.of(LocalDate.of(1970, 1, 1), LocalTime.of(18, 45), ZoneOffset.UTC);
    Assert.assertEquals(timeOnlyUTC, rows.get(0).getValue("date13"));

    ZonedDateTime timeWithZoneUTC = ZonedDateTime.of(LocalDate.of(1970, 1, 2), LocalTime.of(2, 45), ZoneOffset.UTC);
    Assert.assertEquals(timeWithZoneUTC, rows.get(0).getValue("date14"));

    Assert.assertEquals(pstConverted, rows.get(0).getValue("date15"));
  }

  @Test
  public void testDateConversionToLong() throws Exception {
    String[] directives = new String[] {
        "parse-as-simple-date date yyyy-MM-dd'T'HH:mm:ss"
    };

    List<Row> rows = Arrays.asList(
        new Row("date", "2017-02-02T21:06:44"));

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
  }

  @Test
  public void testDateParser() throws Exception {
    String[] directives = new String[] {
        "parse-as-date date US/Eastern",
        "format-date date_1 MM/dd/yyyy HH:mm"
    };

    List<Row> rows = Arrays.asList(
        new Row("date", "now"),
        new Row("date", "today"),
        new Row("date", "12/10/2016"),
        new Row("date", "12/10/2016 06:45 AM"),
        new Row("date", "september 7th 2016"),
        new Row("date", "1485800109"));

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(6, rows.size());
  }

  @Test
  public void testFormatDate() throws Exception {
    String[] directives = new String[] {
        "parse-as-simple-date date1 MM/dd/yyyy",
        "format-date date1 MM/dd/yyyy",
        "parse-as-simple-date date2 dd/MM/yyyy",
        "format-date date2 dd/MM/yyyy",
        "parse-as-simple-date date3 MM-dd-yyyy",
        "format-date date3 MM-dd-yyyy",
        "parse-as-simple-date date4 MM-dd-yy",
        "format-date date4 MM-dd-yy",
        "parse-as-simple-date date5 yyyy-MM-dd",
        "format-date date5 yyyy-MM-dd",
        "parse-as-simple-date date6 yyyy-MM-dd HH:mm:ss",
        "format-date date6 yyyy-MM-dd HH:mm:ss",
        "parse-as-simple-date date7 MM-dd-yyyy 'at' HH:mm:ss z",
        "format-date date7 MM-dd-yyyy 'at' HH:mm:ss z",
        "parse-as-simple-date date8 dd/MM/yy HH:mm:ss",
        "format-date date8 dd/MM/yy HH:mm:ss",
        "parse-as-simple-date date9 yyyy,MM.dd'T'HH:mm:ss.SSSZ",
        "format-date date9 yyyy,MM.dd'T'HH:mm:ss.SSSZ",
        "parse-as-simple-date date10 MM.dd.yyyy HH:mm:ss.SSS",
        "format-date date10 MM.dd.yyyy HH:mm:ss.SSS",
        "parse-as-simple-date date11 EEE, d MMM yyyy HH:mm:ss",
        "format-date date11 EEE, d MMM yyyy HH:mm:ss",
        "parse-as-simple-date date12 EEE, MMM d, ''yy",
        "format-date date12 EEE, MMM d, ''yy",
        "parse-as-simple-date date15 yyyy.MM.dd G 'at' HH:mm:ss z",
        "format-date date15 yyyy.MM.dd G 'at' HH:mm:ss z"
    };

    Row row = new Row();
    row.add("date1", "12/10/2016");
    row.add("date2", "10/12/2016");
    row.add("date3", "12-10-2016");
    row.add("date4", "12-10-16");
    row.add("date5", "2016-12-10");
    row.add("date6", "2016-12-10 06:45:11");
    row.add("date7", "12-10-2016 at 06:45:11 PST");
    row.add("date8", "10/12/2016 06:45:11");
    row.add("date9", "2016,12.10T06:45:11.111-0800");
    row.add("date10", "12.10.2016 06:45:11.111");
    row.add("date11", "Sat, 10 Dec 2016 06:45:11");
    row.add("date12", "Sat, Dec 10, '16");
    row.add("date15", "2016.12.10 AD at 06:45:11 PST");

    List<Row> rows = TestingRig.execute(directives, Arrays.asList(row));

    Assert.assertEquals("12/10/2016", rows.get(0).getValue("date1"));
    Assert.assertEquals("10/12/2016", rows.get(0).getValue("date2"));
    Assert.assertEquals("12-10-2016", rows.get(0).getValue("date3"));
    Assert.assertEquals("12-10-16", rows.get(0).getValue("date4"));
    Assert.assertEquals("2016-12-10", rows.get(0).getValue("date5"));
    Assert.assertEquals("2016-12-10 06:45:11", rows.get(0).getValue("date6"));
    Assert.assertEquals("12-10-2016 at 14:45:11 UTC", rows.get(0).getValue("date7"));
    Assert.assertEquals("10/12/16 06:45:11", rows.get(0).getValue("date8"));
    Assert.assertEquals("2016,12.10T14:45:11.111+0000", rows.get(0).getValue("date9"));
    Assert.assertEquals("12.10.2016 06:45:11.111", rows.get(0).getValue("date10"));
    Assert.assertEquals("Sat, 10 Dec 2016 06:45:11", rows.get(0).getValue("date11"));
    Assert.assertEquals("Sat, Dec 10, '16", rows.get(0).getValue("date12"));
    Assert.assertEquals("2016.12.10 AD at 14:45:11 UTC", rows.get(0).getValue("date15"));
  }
}
