/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.parser.ParseDate;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link ParseDate}
 */
public class ParseDateTest {

  @Test
  public void testBasicDateParser() throws Exception {
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

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 6);
  }

}