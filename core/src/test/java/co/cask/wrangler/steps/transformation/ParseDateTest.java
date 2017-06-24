/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.RecipePipelineTest;
import co.cask.wrangler.steps.parser.ParseDate;
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

    Record record1 = new Record();
    // 1 hour diff
    record1.add("date1", "12/10/2016 07:45");
    record1.add("date2", "12/10/2016 06:45");

    // 1 month and 1 second diff
    Record record2 = new Record();
    record2.add("date1", "2/1/1990 12:01");
    record2.add("date2", "1/1/1990 12:00");

    // no diff
    Record record3 = new Record();
    record3.add("date1", "03/03/1998 2:02");
    record3.add("date2", "03/03/1998 2:02");

    List<Record> records = RecipePipelineTest.execute(directives, Arrays.asList(record1, record2, record3));

    Assert.assertEquals(TimeUnit.HOURS.toMillis(1), records.get(0).getValue("difference"));
    Assert.assertEquals(2678460000L, records.get(1).getValue("difference"));
    Assert.assertEquals(0L, records.get(2).getValue("difference"));
    Assert.assertTrue(records.size() == 3);
  }

  @Test
  public void testDateConversionToLong() throws Exception {
    String[] directives = new String[] {
      "parse-as-simple-date date yyyy-MM-dd'T'HH:mm:ss"
    };

    //2017-02-02T21:06:44Z
    List<Record> records = Arrays.asList(
      new Record("date", "2017-02-02T21:06:44Z")
    );

    records = RecipePipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 1);
  }

  @Test
  public void testDateParser() throws Exception {
    String[] directives = new String[] {
      "parse-as-date date US/Eastern",
      "format-date date_1 MM/dd/yyyy HH:mm"
    };

    List<Record> records = Arrays.asList(
      new Record("date", "now"),
      new Record("date", "today"),
      new Record("date", "12/10/2016"),
      new Record("date", "12/10/2016 06:45 AM"),
      new Record("date", "september 7th 2016"),
      new Record("date", "1485800109")
    );

    records = RecipePipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 6);
  }

}