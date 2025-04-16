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
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link ParseDate}
 */
public class ParseDateTest {

  @Test
  public void testSimpleDateParserAndDiff() throws Exception {
    String[] directives = new String[] {
      "parse-as-simple-date date1 MM/dd/yyyy HH:mm",
      "parse-as-simple-date date2 MM/dd/yyyy HH:mm",
      "diff-date date1 date2 difference"
    };

    Row row1 = new Row();
    // 1 hour diff
    row1.add("date1", "12/10/2016 07:45");
    row1.add("date2", "12/10/2016 06:45");

    // 1 month and 1 second diff
    Row row2 = new Row();
    row2.add("date1", "2/1/1990 12:01");
    row2.add("date2", "1/1/1990 12:00");

    // no diff
    Row row3 = new Row();
    row3.add("date1", "03/03/1998 2:02");
    row3.add("date2", "03/03/1998 2:02");

    List<Row> rows = TestingRig.execute(directives, Arrays.asList(row1, row2, row3));

    Assert.assertEquals(TimeUnit.HOURS.toMillis(1), rows.get(0).getValue("difference"));
    Assert.assertEquals(2678460000L, rows.get(1).getValue("difference"));
    Assert.assertEquals(0L, rows.get(2).getValue("difference"));
    Assert.assertTrue(rows.size() == 3);
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
      "parse-as-simple-date date15 yyyy.MM.dd G 'at' HH:mm:ss z",
    };

    Row row1 = new Row();
    // MM/dd/yyyy
    row1.add("date1", "12/10/2016");
    // dd/MM/yyyy
    row1.add("date2", "10/12/2016");
    // MM-dd-yyyy
    row1.add("date3", "12-10-2016");
    // MM-dd-yy
    row1.add("date4", "12-10-16");
    // yyyy-MM-dd
    row1.add("date5", "2016-12-10");
    // yyyy-MM-dd HH:mm:ss
    row1.add("date6", "2016-12-10 06:45:11");
    // MM-dd-yyyy 'at' HH:mm:ss with timezone
    row1.add("date7", "12-10-2016 at 06:45:11 PST");
    // dd/MM/yy HH:mm:ss
    row1.add("date8", "10/12/2016 06:45:11");
    // yyyy,MM.dd'T'HH:mm:ss.SSS with RFC timezone
    row1.add("date9", "2016,12.10T06:45:11.111-0800");
    // MM.dd.yyyy HH:mm:ss.SSS
    row1.add("date10", "12.10.2016 06:45:11.111");
    // EEE, d MMM yyyy HH:mm:ss
    row1.add("date11", "Sat, 10 Dec 2016 06:45:11");
    // EEE, MMM d, 'yy
    row1.add("date12", "Sat, Dec 10, '16");
    // h:mm AM/PM
    row1.add("date13", "06:45 PM");
    // H:mm with timezone
    row1.add("date14", "06:45 PM, PST");
    // Custom - yyyy.MM.dd G 'at' HH:mm:ss z
    row1.add("date15", "2016.12.10 AD at 06:45:11 PST");

    List<Row> rows = TestingRig.execute(directives, Arrays.asList(row1));
    LocalDate localDate = LocalDate.of(2016, 12, 10);
    LocalTime zeroTime = LocalTime.of(0, 0);
    ZonedDateTime zonedDateZeroTime = ZonedDateTime.of(localDate, zeroTime, ZoneId.ofOffset("UTC", ZoneOffset.UTC));

    Assert.assertEquals(zonedDateZeroTime, rows.get(0).getValue("date1"));
    Assert.assertEquals(zonedDateZeroTime, rows.get(0).getValue("date2"));
    Assert.assertEquals(zonedDateZeroTime, rows.get(0).getValue("date3"));
    Assert.assertEquals(zonedDateZeroTime, rows.get(0).getValue("date4"));
    Assert.assertEquals(zonedDateZeroTime, rows.get(0).getValue("date5"));

    LocalTime localTime = LocalTime.of(6, 45, 11);
    ZonedDateTime zonedDateTime = ZonedDateTime.of(localDate, localTime, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    ZonedDateTime pstDateTime = ZonedDateTime.of(localDate, LocalTime.of(14, 45, 11),
                                                 ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    Assert.assertEquals(zonedDateTime, rows.get(0).getValue("date6"));
    Assert.assertEquals(pstDateTime, rows.get(0).getValue("date7"));
    Assert.assertEquals(zonedDateTime, rows.get(0).getValue("date8"));
    Assert.assertEquals(pstDateTime.plusNanos(TimeUnit.SECONDS.toMicros(111)), rows.get(0).getValue("date9"));
    Assert.assertEquals(zonedDateTime.plusNanos(TimeUnit.SECONDS.toMicros(111)), rows.get(0).getValue("date10"));
    Assert.assertEquals(zonedDateTime, rows.get(0).getValue("date11"));
    Assert.assertEquals(zonedDateZeroTime, rows.get(0).getValue("date12"));
    Assert.assertEquals(ZonedDateTime.of(LocalDate.of(1970, 1, 1), LocalTime.of(18, 45),
                                         ZoneId.ofOffset("UTC", ZoneOffset.UTC)),
                        rows.get(0).getValue("date13"));
    Assert.assertEquals(ZonedDateTime.of(LocalDate.of(1970, 1, 2), LocalTime.of(2, 45),
                                         ZoneId.ofOffset("UTC", ZoneOffset.UTC)),
                        rows.get(0).getValue("date14"));
    Assert.assertEquals(pstDateTime, rows.get(0).getValue("date15"));
  }

  @Test
  public void testDateConversionToLong() throws Exception {
    String[] directives = new String[] {
      "parse-as-simple-date date yyyy-MM-dd'T'HH:mm:ss"
    };

    //2017-02-02T21:06:44Z
    List<Row> rows = Arrays.asList(
      new Row("date", "2017-02-02T21:06:44Z")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
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
      new Row("date", "1485800109")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 6);
    // TODO CDAP-14243 - add more tests once the issue with parser is fixed
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

    Row row1 = new Row();
    // MM/dd/yyyy
    row1.add("date1", "12/10/2016");
    // dd/MM/yyyy
    row1.add("date2", "10/12/2016");
    // MM-dd-yyyy
    row1.add("date3", "12-10-2016");
    // MM-dd-yy
    row1.add("date4", "12-10-16");
    // yyyy-MM-dd
    row1.add("date5", "2016-12-10");
    // yyyy-MM-dd HH:mm:ss
    row1.add("date6", "2016-12-10 06:45:11");
    // MM-dd-yyyy 'at' HH:mm:ss with timezone
    row1.add("date7", "12-10-2016 at 06:45:11 PST");
    // dd/MM/yy HH:mm:ss
    row1.add("date8", "10/12/2016 06:45:11");
    // yyyy,MM.dd'T'HH:mm:ss.SSS with RFC timezone
    row1.add("date9", "2016,12.10T06:45:11.111-0800");
    // MM.dd.yyyy HH:mm:ss.SSS
    row1.add("date10", "12.10.2016 06:45:11.111");
    // EEE, d MMM yyyy HH:mm:ss
    row1.add("date11", "Sat, 10 Dec 2016 06:45:11");
    // EEE, MMM d, 'yy
    row1.add("date12", "Sat, Dec 10, '16");
    // Custom - yyyy.MM.dd G 'at' HH:mm:ss z
    row1.add("date15", "2016.12.10 AD at 06:45:11 PST");

    List<Row> rows = TestingRig.execute(directives, Arrays.asList(row1));
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
