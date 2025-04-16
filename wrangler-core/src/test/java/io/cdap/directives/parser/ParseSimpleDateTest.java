/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.directives.parser;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.iq80.leveldb.shaded.guava.collect.Streams;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParseSimpleDateTest {

  @Test
  public void testPatterns() throws Exception {
    List<String> patterns = Arrays.asList(
      "yyyy.MM.dd G 'at' HH:mm:ss z",
      "EEE, MMM d, ''yy",
      "h:mm a",
      "hh 'o''clock' a, zzzz",
      "K:mm a, z",
      "yyyy.MMMMM.dd GGG hh:mm aaa",
      "EEE, d MMM yyyy HH:mm:ss Z",
      "yyMMddHHmmssZ",
      "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
      "MM/dd/yyyy HH:mm",
      "yyyy.MM.dd"
    );
    List<String> dates = Arrays.asList(
      "2001.07.04 AD at 12:08:56 PDT",
      "Wed, Jul 4, '01",
      "12:08 PM",
      "12 o'clock PM, Pacific Daylight Time",
      "0:08 PM, PDT",
      "2001.July.04 AD 12:08 PM",
      "Wed, 4 Jul 2001 12:08:56 -0700",
      "010704120856-0700",
      "2001-07-04T12:08:56.235-0700",
      "2001-07-04T12:08:56.235-07:00",
      "07/04/2001 12:09",
      "2001.07.04"
    );
    int numValues = patterns.size();
    List<String> columns = IntStream.range(0, numValues).mapToObj(i -> "col" + i).collect(Collectors.toList());
    String[] directives = Streams
      .zip(
        columns.stream(),
        patterns.stream(),
        (column, pattern) -> String.format("%s :%s \"%s\"", ParseSimpleDate.NAME, column, pattern)
      ).toArray(String[]::new);
    Row row = new Row();
    for (int i = 0; i < numValues; i++) {
      row.add(columns.get(i), dates.get(i));
    }

    List<Row> resultRows = TestingRig.execute(directives, Collections.singletonList(row));

    Assert.assertEquals(1, resultRows.size());
    for (int i = 0; i < numValues; i++) {
      SimpleDateFormat formatter = new SimpleDateFormat(patterns.get(i));
      formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

      ZonedDateTime zonedDateTime = ZonedDateTime.from(
        formatter.parse(dates.get(i)).toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC))
      );
      Assert.assertEquals(zonedDateTime, resultRows.get(0).getValue(columns.get(i)));
    }
  }

  @Test
  public void testUTCTimeZoneUsedByDefault() throws Exception {
    String pattern = "yyyy-MM-dd";
    String column = "col";
    List<String> dates = Arrays.asList("1850-09-09", "1998-09-04", "2018-05-14");
    String[] directives = new String[]{String.format("%s :%s \"%s\"", ParseSimpleDate.NAME, column, pattern)};
    List<Row> rows = dates.stream().map(date -> new Row(column, date)).collect(Collectors.toList());

    List<Row> resultRows = TestingRig.execute(directives, rows);

    Assert.assertEquals(dates.size(), resultRows.size());
    SimpleDateFormat formatter = new SimpleDateFormat(pattern);
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    for (int i = 0; i < dates.size(); i++) {
      ZonedDateTime zonedDateTime = ZonedDateTime.from(
        formatter.parse(dates.get(i)).toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC))
      );
      Assert.assertEquals(zonedDateTime, resultRows.get(i).getValue(column));
    }
  }

  @Test
  public void testGregorianCutoff() throws Exception {
    String pattern = "yyyy-MM-dd HH:mm:ss";
    String column = "col";
    List<String> dates = Arrays.asList(
      "0001-01-01 00:00:00",
      "1000-01-01 00:00:00",
      "1500-01-10 00:00:00",
      "1582-10-14 23:59:59",
      "1582-10-15 00:00:00",
      "1850-09-09 00:00:00",
      "1998-09-04 00:00:00",
      "2018-05-14 00:00:00"
    );
    String[] directives = new String[]{String.format("%s :%s \"%s\"", ParseSimpleDate.NAME, column, pattern)};
    List<Row> rows = dates.stream().map(date -> new Row(column, date)).collect(Collectors.toList());

    List<Row> resultRows = TestingRig.execute(directives, rows);

    Assert.assertEquals(dates.size(), resultRows.size());

    GregorianCalendar gc = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    gc.setGregorianChange(new Date(Long.MIN_VALUE));
    SimpleDateFormat formatter = new SimpleDateFormat(pattern);
    formatter.setCalendar(gc);
    for (int i = 0; i < dates.size(); i++) {
      ZonedDateTime zonedDateTime = ZonedDateTime.from(
        formatter.parse(dates.get(i)).toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC))
      );
      Assert.assertEquals(zonedDateTime, resultRows.get(i).getValue(column));
    }
    // For good measure :)
    Assert.assertEquals("0001-01-01T00:00Z[UTC]", resultRows.get(0).getValue(column).toString());
  }

  @Test(expected = RecipeException.class)
  public void testInvalidFormatterPatternThrowsRecipeException() throws Exception {
    String pattern = "foobar";
    String column = "col";
    String date = "2001-07-04";
    String[] directives = new String[]{String.format("%s :%s \"%s\"", ParseSimpleDate.NAME, column, pattern)};
    Row row = new Row(column, date);

    TestingRig.execute(directives, Collections.singletonList(row));
  }

  @Test
  public void testParseExceptionCollectsAsErrorRecord() throws Exception {
    String column = "col";
    String date = "2001-07-04";
    String pattern = "yyyy.MM.dd";
    String[] directives = new String[]{String.format("%s :%s \"%s\"", ParseSimpleDate.NAME, column, pattern)};
    Row row = new Row(column, date);

    Pair<List<Row>, List<Row>> result = TestingRig.executeWithErrors(directives, Collections.singletonList(row));

    List<Row> resultRows = result.getFirst();
    List<Row> errorRows = result.getSecond();
    Assert.assertEquals(0, resultRows.size());
    Assert.assertEquals(1, errorRows.size());
  }
}
