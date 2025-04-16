/*
 *  Copyright © 2018-2019 Cask Data, Inc.
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

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ParseTimestampTest {
  @Test
  public void testParseTimestamp() throws Exception {
    String[] directives = new String[] {
      "parse-timestamp :date1",
      "parse-timestamp :date2",
      "parse-timestamp :date3",
      "parse-timestamp :date4 'seconds'",
      "parse-timestamp :date5 'milliseconds'",
      "parse-timestamp :date6 'microseconds'"
    };

    Row row1 = new Row();
    row1.add("date1", 1536332271894L);
    row1.add("date2", null);
    row1.add("date3", "1536332271894");
    row1.add("date4", "1536332271");
    row1.add("date5", "1536332271894");
    row1.add("date6", "1536332271894123");

    List<Row> rows = TestingRig.execute(directives, Arrays.asList(row1));
    ZonedDateTime dateTime = ZonedDateTime.of(2018, 9, 7, 14, 57, 51,
                                              Math.toIntExact(TimeUnit.MILLISECONDS.toNanos(894)),
                                              ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    Assert.assertEquals(dateTime, rows.get(0).getValue("date1"));
    Assert.assertNull(rows.get(0).getValue("date2"));
    Assert.assertEquals(dateTime, rows.get(0).getValue("date3"));
    Assert.assertEquals(dateTime.minusNanos(TimeUnit.MILLISECONDS.toNanos(894)), rows.get(0).getValue("date4"));
    Assert.assertEquals(dateTime, rows.get(0).getValue("date5"));
    Assert.assertEquals(dateTime.plusNanos(TimeUnit.MILLISECONDS.toMicros(123)), rows.get(0).getValue("date6"));
  }

  @Test(expected = RecipeException.class)
  public void testInvalidTimestamp() throws Exception {
    String[] directives = new String[] {
      "parse-timestamp :date1 'nanoseconds'"
    };

    Row row1 = new Row();
    row1.add("date1", 1536332271894L);

    List<Row> rows = TestingRig.execute(directives, Arrays.asList(row1));
  }
}
