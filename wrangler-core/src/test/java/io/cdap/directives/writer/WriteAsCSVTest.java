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

package io.cdap.directives.writer;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.StringMetrics;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link WriteAsCSV}
 */
public class WriteAsCSVTest {

  @Test
  public void testWriteAsCSV() throws Exception {
    String[] directives = new String[]{
      "write-as-csv test",
    };

    JSONObject o = new JSONObject();
    o.put("a", 1);
    o.put("b", "2");
    List<Row> rows = Arrays.asList(
      new Row("url", "http://www.yahoo.com?a=b c&b=ab&xyz=1")
        .add("o", o)
        .add("i1", new Integer(1))
        .add("i2", new Double(1.8f))
    );
    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    StringMetric metric = StringMetrics.euclideanDistance();

    // Due to double, the string comparision might not be appropriate.
    float value = metric.compare("http://www.yahoo.com?a=b c&b=ab&xyz=1,\"{\"\"b\"\":\"\"2\"\",\"\"a\"\":1}\"," +
                                   "1,1.7999999523162842", (String) rows.get(0).getValue(4));
    Assert.assertTrue(value > 0.4);
  }

  @Test
  public void testExample() throws Exception {
    String[] directives = new String[]{
      "write-as-csv body",
    };

    List<Row> rows = Arrays.asList(
      new Row("int", 1).add("string", "this is, string")
    );
    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("1,\"this is, string\"", rows.get(0).getValue(2));
  }

}
