/*
 *  Copyright © 2021 Cask Data, Inc.
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
package io.cdap.directives.datetime;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

public class DateTimeToTimestampTest {

  @Test
  public void testZones() throws Exception {
    String[] testZones = new String[]{"UTC", "GMT", "Australia/Sydney", "America/Los_Angeles"};
    String[] colNames = new String[]{"col1", "col2", "col3", "col4"};
    LocalDateTime localDateTime = LocalDateTime.of(2000, 8, 22, 20, 36, 45, 1234);
    String[] directives = new String[testZones.length];
    Row row = new Row();
    for (int i = 0; i < testZones.length; i++) {
      directives[i] = String.format("%s :%s \"%s\"", DateTimeToTimeStamp.NAME, colNames[i], testZones[i]);
      row.add(colNames[i], localDateTime);
    }
    List<Row> rows = TestingRig.execute(directives, Collections.singletonList(row));

    Assert.assertEquals(1, rows.size());

    for (Row resultRow : rows) {
      for (int i = 0; i < testZones.length; i++) {
        Assert.assertEquals(ZonedDateTime.of(localDateTime, ZoneId.of(testZones[i])),
                            rows.get(0).getValue(colNames[i]));
      }
    }
  }

  @Test
  public void testDefaultZone() throws Exception {
    String colName = "col1";
    String[] directives = new String[]{String.format("%s :%s", DateTimeToTimeStamp.NAME, colName)};
    Row row1 = new Row();
    LocalDateTime now = LocalDateTime.now();
    row1.add(colName, now);
    List<Row> result = TestingRig.execute(directives, Collections.singletonList(row1));
    Assert.assertEquals(ZonedDateTime.of(now, ZoneId.of("UTC")),
                        result.get(0).getValue(colName));
  }

  @Test(expected = RecipeException.class)
  public void testInvalidZone() throws Exception {
    String zone = "abcd";
    String colName = "col1";
    String[] directives = new String[]{String.format("%s :%s '%s'", DateTimeToTimeStamp.NAME, colName, zone)};
    Row row1 = new Row();
    row1.add(colName, LocalDateTime.now());
    TestingRig.execute(directives, Collections.singletonList(row1));
  }

  @Test
  public void testInvalidObject() throws Exception {
    String colName = "col1";
    String[] directives = new String[]{String.format("%s :%s", DateTimeToTimeStamp.NAME, colName)};
    Row row1 = new Row();
    row1.add(colName, LocalDateTime.now().toString());
    final List<Row> results = TestingRig.execute(directives, Collections.singletonList(row1));
    //should be error collected
    Assert.assertTrue(results.isEmpty());
  }
}
