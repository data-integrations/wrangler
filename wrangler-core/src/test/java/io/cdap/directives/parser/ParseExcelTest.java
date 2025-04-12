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

package io.cdap.directives.parser;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.Row;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link ParseExcel}
 */
public class ParseExcelTest {

  @Test
  public void testBasicExcel() throws Exception {
    try (InputStream stream = ParseAvroFileTest.class.getClassLoader().getResourceAsStream("titanic.xlsx")) {
      byte[] data = IOUtils.toByteArray(stream);

      String[] directives = new String[]{
        "parse-as-excel :body '0'",
      };

      List<Row> rows = new ArrayList<>();
      rows.add(new Row("body", data));

      List<Row> results = TestingRig.execute(directives, rows);
      Assert.assertEquals(892, results.size());
      Assert.assertEquals(0, results.get(0).getValue("fwd"));
      Assert.assertEquals(891, results.get(0).getValue("bkd"));
    }
  }

  @Test
  public void testNoSheetName() throws Exception {
    try (InputStream stream = ParseAvroFileTest.class.getClassLoader().getResourceAsStream("titanic.xlsx")) {
      byte[] data = IOUtils.toByteArray(stream);

      String[] directives = new String[]{
        "parse-as-excel :body 'wrong_error'",
      };

      List<Row> rows = new ArrayList<>();
      rows.add(new Row("body", data));
      Pair<List<Row>, List<Row>> pipeline = TestingRig.executeWithErrors(directives, rows);
      Assert.assertEquals(0, pipeline.getFirst().size());
      Assert.assertEquals(1, pipeline.getSecond().size());
    }
  }

  @Test
  public void testDateFormatting() throws Exception {
    try (InputStream stream =
           ParseAvroFileTest.class.getClassLoader().getResourceAsStream("date-formats-test-sheet.xlsx")) {
      byte[] data = IOUtils.toByteArray(stream);

      String[] directives = new String[]{
        "parse-as-excel :body '0'",
      };

      List<Row> rows = new ArrayList<>();
      rows.add(new Row("body", data));
      List<Row> results = TestingRig.execute(directives, rows);

      for (Row result : results) {
        Assert.assertEquals(result.getValue("A"), result.getValue("B"));
      }
    }
  }
}
