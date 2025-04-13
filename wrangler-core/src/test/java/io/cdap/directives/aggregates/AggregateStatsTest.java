/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.directives.aggregates;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link AggregateStats}
 */
public class AggregateStatsTest {

  @Test
  public void testSimpleAggregation() throws Exception {
    String[] directives = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> rows = Arrays.asList(
      new Row("data_transfer_size", new ByteSize("1MB"))
        .add("response_time", new TimeDuration("1s")),
      new Row("data_transfer_size", new ByteSize("2MB"))
        .add("response_time", new TimeDuration("2s"))
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(3.0, rows.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(3.0, rows.get(0).getValue("total_time_sec"), 0.001);
  }

  @Test
  public void testMixedUnits() throws Exception {
    String[] directives = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> rows = Arrays.asList(
      new Row("data_transfer_size", new ByteSize("1024KB"))
        .add("response_time", new TimeDuration("1000ms")),
      new Row("data_transfer_size", new ByteSize("1MB"))
        .add("response_time", new TimeDuration("1s"))
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(2.0, rows.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(2.0, rows.get(0).getValue("total_time_sec"), 0.001);
  }

  @Test
  public void testLargeValues() throws Exception {
    String[] directives = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> rows = Arrays.asList(
      new Row("data_transfer_size", new ByteSize("1GB"))
        .add("response_time", new TimeDuration("1h")),
      new Row("data_transfer_size", new ByteSize("1TB"))
        .add("response_time", new TimeDuration("1m"))
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    // 1GB + 1TB in MB
    Assert.assertEquals(1024 + 1024 * 1024, rows.get(0).getValue("total_size_mb"), 0.001);
    // 1h + 1m in seconds
    Assert.assertEquals(3660.0, rows.get(0).getValue("total_time_sec"), 0.001);
  }

  @Test
  public void testNullValues() throws Exception {
    String[] directives = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> rows = Arrays.asList(
      new Row("data_transfer_size", null)
        .add("response_time", new TimeDuration("1s")),
      new Row("data_transfer_size", new ByteSize("1MB"))
        .add("response_time", null)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(1.0, rows.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(1.0, rows.get(0).getValue("total_time_sec"), 0.001);
  }

  @Test(expected = Exception.class)
  public void testInvalidValues() throws Exception {
    String[] directives = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> rows = Arrays.asList(
      new Row("data_transfer_size", "invalid")
        .add("response_time", new TimeDuration("1s"))
    );

    TestingRig.execute(directives, rows);
  }

  @Test
  public void testEmptyRowSet() throws Exception {
    String[] directives = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> rows = Arrays.asList();
    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(0.0, rows.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(0.0, rows.get(0).getValue("total_time_sec"), 0.001);
  }
}
