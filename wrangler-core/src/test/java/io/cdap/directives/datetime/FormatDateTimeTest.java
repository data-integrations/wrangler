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
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
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
    String[] testPatterns = new String[]{
      "MM/dd/yyyy HH:mm",
      "yyyy-MM-dd'T'HH:mm:ss",
      "yyyy-MM-dd'T'HH:mm:ss",
      "yyyyMMdd h:mm a"
    };

    String[] colNames = new String[]{"col1", "col2", "col3", "col4"};
    LocalDateTime localDateTime = LocalDateTime.of(2000, 8, 22, 20, 36, 45, 1234);

    String[] expectedFormattedDates = new String[]{
      "08/22/2000 20:36",
      "2000-08-22T20:36:45",
      "2000-08-22T20:36:45",
      "20000822 8:36 PM"
    };

    String[] directives = new String[testPatterns.length];
    Row inputRow = new Row();

    for (int i = 0; i < testPatterns.length; i++) {
      directives[i] = String.format("format-datetime :%s '%s'", colNames[i], testPatterns[i]);
      inputRow.add(colNames[i], localDateTime);
    }

    List<Row> rows = TestingRig.execute(directives, Collections.singletonList(inputRow));

    Assert.assertEquals("Expected only one output row", 1, rows.size());
    Row resultRow = rows.get(0);

    for (int i = 0; i < colNames.length; i++) {
      String actual = (String) resultRow.getValue(colNames[i]);
      Assert.assertEquals(
        String.format("Mismatch for column '%s' with pattern '%s'", colNames[i], testPatterns[i]),
        expectedFormattedDates[i],
        actual
      );
    }
  }

  @Test(expected = RecipeException.class)
  public void testInvalidFormat() throws Exception {
    String pattern = "abcd";
    String colName = "col1";
    String[] directives = new String[]{
      String.format("format-datetime :%s '%s'", colName, pattern)
    };

    Row row = new Row();
    row.add(colName, LocalDateTime.now());

    TestingRig.execute(directives, Collections.singletonList(row));
  }

  @Test
  public void testInvalidObject() throws Exception {
    String pattern = "MM/dd/yyyy HH:mm";
    String colName = "col1";
    String invalidDateTime = "12/10/2016"; // Invalid input, expected LocalDateTime, got String

    String[] directives = new String[]{
      String.format("format-datetime :%s '%s'", colName, pattern)
    };

    Row row = new Row();
    row.add(colName, invalidDateTime);

    List<Row> results = TestingRig.execute(directives, Collections.singletonList(row));

    // The row should be filtered out because of an invalid type
    Assert.assertTrue("Expected no results for invalid input type", results.isEmpty());
  }
}
