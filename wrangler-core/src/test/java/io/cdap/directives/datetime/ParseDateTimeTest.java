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
import io.cdap.wrangler.api.DirectiveParseException;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ParseDateTimeTest {

  @Test
  public void testDateTimeFormats() throws Exception {
    String[] directives = new String[]{
      "parse-as-datetime :date 'yyyy-MM-dd HH:mm:ss'",
    };

    List<Row> rows = Arrays.asList(
      new Row("date", "2020-01-01 12:00:00")
    );

    List<Row> results = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(LocalDateTime.of(2020, 1, 1, 12, 0, 0), results.get(0).getValue("date"));
  }

  @Test
  public void testDateTimeMultipleRows() throws Exception {
    String pattern = "MM/dd/yyyy HH:mm";
    String colName = "col1";
    String datetime1 = "12/10/2016 07:45";
    String datetime2 = "02/01/1990 12:01";
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
    String[] directives = new String[]{
      String.format("%s :%s '%s'", ParseDateTime.NAME, colName, pattern)
    };
    Row row1 = new Row();
    row1.add(colName, datetime1);
    Row row2 = new Row();
    row2.add(colName, datetime2);
    List<Row> rows = TestingRig.execute(directives, Arrays.asList(row1, row2));

    Assert.assertEquals(2, rows.size());
    Assert.assertEquals(LocalDateTime.parse(datetime1, dateTimeFormatter),
                        rows.get(0).getValue(colName));
    Assert.assertEquals(LocalDateTime.parse(datetime2, dateTimeFormatter),
                        rows.get(1).getValue(colName));
  }

  @Test(expected = RecipeException.class)
  public void testInvalidFormat() throws Exception {
    String pattern = "abcd";
    String colName = "col1";
    String datetime1 = "12/10/2016 07:45";
    String[] directives = new String[]{
      String.format("%s :%s '%s'", ParseDateTime.NAME, colName, pattern)
    };
    Row row1 = new Row();
    row1.add(colName, datetime1);
    try {
      TestingRig.execute(directives, Collections.singletonList(row1));
    } catch (DirectiveParseException e) {
      throw new RecipeException(e.getMessage(), e);
    }
  }

  @Test
  public void testInvalidData() throws Exception {
    String pattern = "MM/dd/yyyy HH:mm";
    String colName = "col1";
    String datetime1 = "12/10/2016";
    String[] directives = new String[]{
      String.format("%s :%s '%s'", ParseDateTime.NAME, colName, pattern)
    };
    Row row1 = new Row();
    row1.add(colName, datetime1);
    final List<Row> results = TestingRig.execute(directives, Collections.singletonList(row1));
    //should be error collected
    Assert.assertTrue(results.isEmpty());
  }
}
