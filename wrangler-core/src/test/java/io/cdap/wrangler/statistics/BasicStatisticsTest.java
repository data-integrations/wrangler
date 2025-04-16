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

package io.cdap.wrangler.statistics;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import io.dataapps.chlorine.finder.FinderEngine;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link BasicStatistics}
 */
public class BasicStatisticsTest {

  @Test
  public void testFinderXMLFormat() throws Exception {
    FinderEngine engine = new FinderEngine("wrangler-finder.xml", true, false);
    Assert.assertNotNull(engine);
    Assert.assertTrue(engine.getFinders().size() > 5); // 5 is minimal.
  }

  @Test
  public void testMetaBasic() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "drop body"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "1234.45,650-897-3839,111-11-1111,32826,02/29/2000,\"$1234.56\",http://www.yahoo.com"),
      new Row("body", "45.56,670-897-3839,111-12-1111,32826,02/01/2011,\"$56,789\",http://mars.io"),
      new Row("body", "45.56,670-897-3839,222,32826,9/14/2016,\"\",http://mars.io")
    );

    rows = TestingRig.execute(directives, rows);

    Statistics meta = new BasicStatistics();
    Row summary = meta.aggregate(rows);

    Assert.assertTrue(rows.size() > 1);

    Assert.assertEquals(3, summary.width());
    Assert.assertEquals(3.0, summary.getValue("total"));

    Row stats = (Row) summary.getValue("stats");
    Row types = (Row) summary.getValue("types");

    Assert.assertEquals(7, stats.width());
    Assert.assertEquals(7, types.width());
  }
}

