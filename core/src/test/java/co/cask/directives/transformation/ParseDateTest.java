/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.transformation;

import co.cask.directives.parser.ParseDate;
import co.cask.TestUtil;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

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

    List<Row> rows = TestUtil.execute(directives, Arrays.asList(row1, row2, row3));

    Assert.assertEquals(TimeUnit.HOURS.toMillis(1), rows.get(0).getValue("difference"));
    Assert.assertEquals(2678460000L, rows.get(1).getValue("difference"));
    Assert.assertEquals(0L, rows.get(2).getValue("difference"));
    Assert.assertTrue(rows.size() == 3);
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

    rows = TestUtil.execute(directives, rows);
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

    rows = TestUtil.execute(directives, rows);

    Assert.assertTrue(rows.size() == 6);
  }

}