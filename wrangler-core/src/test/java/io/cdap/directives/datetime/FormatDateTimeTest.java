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
import java.util.Collections;
import java.util.List;

public class FormatDateTimeTest {

  @Test
  public void testDateTimeFormats() throws Exception {
    String[] testPatterns = new String[]{"MM/dd/yyyy HH:mm", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss[xxx]",
      "yyyyMMdd h:mm a"};
    String[] colNames = new String[]{"col1", "col2", "col3", "col4", "col5"};
    LocalDateTime localDateTime = LocalDateTime.of(2000, 8, 22, 20, 36, 45, 1234);
    String[] dateTimes = new String[]{"08/22/2000 20:36", "2000-08-22T20:36:45", "2000-08-22T20:36:45",
      "20000822 8:36 PM"};
    String[] directives = new String[testPatterns.length];
    Row row = new Row();
    for (int i = 0; i < testPatterns.length; i++) {
      directives[i] = String.format("%s :%s \"%s\"", FormatDateTime.NAME, colNames[i], testPatterns[i]);
      row.add(colNames[i], localDateTime);
    }
    List<Row> rows = TestingRig.execute(directives, Collections.singletonList(row));

    Assert.assertEquals(1, rows.size());
    for (Row resultRow : rows) {
      for (int i = 0; i < testPatterns.length; i++) {
        Assert.assertEquals(dateTimes[i], rows.get(0).getValue(colNames[i]));
      }
    }
  }

  @Test(expected = RecipeException.class)
  public void testInvalidFormat() throws Exception {
    String pattern = "abcd";
    String colName = "col1";
    String[] directives = new String[]{String.format("format-datetime :%s '%s'", colName, pattern)};
    Row row1 = new Row();
    row1.add(colName, LocalDateTime.now());
    TestingRig.execute(directives, Collections.singletonList(row1));
  }

  @Test
  public void testInvalidObject() throws Exception {
    String pattern = "MM/dd/yyyy HH:mm";
    String colName = "col1";
    String datetime1 = "12/10/2016";
    String[] directives = new String[]{String.format("format-datetime :%s '%s'", colName, pattern)};
    Row row1 = new Row();
    row1.add(colName, datetime1);

    final List<Row> results = TestingRig.execute(directives, Collections.singletonList(row1));
    //should be error collected
    Assert.assertTrue(results.isEmpty());
  }
}
