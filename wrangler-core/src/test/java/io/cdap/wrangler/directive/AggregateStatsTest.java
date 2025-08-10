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

package io.cdap.wrangler.directive;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TestingRig;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientStoreScope;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link AggregateStats}
 */
public class AggregateStatsTest {

  @Test
  public void testBasicAggregation() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_size :response_time :total_size :total_time"
    };

    List<Row> rows = new ArrayList<>();
    rows.add(createRow("10KB", "100ms"));
    rows.add(createRow("1.5MB", "2s"));
    rows.add(createRow("2GB", "5m"));

    List<Row> results = TestingRig.execute(recipe, rows);

    Assert.assertEquals(1, results.size());
    Row result = results.get(0);
    
    // Total size should be 2GB + 1.5MB + 10KB = 2147483648 + 1572864 + 10240 bytes
    Assert.assertEquals(2149065735L, result.getValue(":total_size"));
    
    // Total time should be 5m + 2s + 100ms = 300000 + 2000 + 100 milliseconds
    Assert.assertEquals(302100L, result.getValue(":total_time"));
  }

  @Test
  public void testEmptyInput() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_size :response_time :total_size :total_time"
    };

    List<Row> rows = new ArrayList<>();
    List<Row> results = TestingRig.execute(recipe, rows);

    Assert.assertEquals(1, results.size());
    Row result = results.get(0);
    Assert.assertEquals(0L, result.getValue(":total_size"));
    Assert.assertEquals(0L, result.getValue(":total_time"));
  }

  @Test
  public void testNullValues() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_size :response_time :total_size :total_time"
    };

    List<Row> rows = new ArrayList<>();
    rows.add(createRow(null, "100ms"));
    rows.add(createRow("1.5MB", null));
    rows.add(createRow(null, null));

    List<Row> results = TestingRig.execute(recipe, rows);

    Assert.assertEquals(1, results.size());
    Row result = results.get(0);
    Assert.assertEquals(1572864L, result.getValue(":total_size")); // Only 1.5MB
    Assert.assertEquals(100L, result.getValue(":total_time")); // Only 100ms
  }

  private Row createRow(String size, String time) {
    Row row = new Row();
    row.add(":data_size", size);
    row.add(":response_time", time);
    return row;
  }
} 