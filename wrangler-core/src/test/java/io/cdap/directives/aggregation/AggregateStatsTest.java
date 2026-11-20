/*
 * Copyright © 2023-2025 Cask Data, Inc.
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

package io.cdap.directives.aggregation;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.Row;
// import io.cdap.wrangler.executor.RecipePipeline;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link AggregateStats}
 */
public class AggregateStatsTest {

  @Test
  public void testTotalAggregation() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> rows = new ArrayList<>();
    rows.add(createRow("10KB", "500ms", "record1"));
    rows.add(createRow("20KB", "300ms", "record2"));
    rows.add(createRow("5MB", "1.2s", "record3"));

    RecipePipeline executor = TestingRig.execute(recipe);
    List<Row> results = executor.execute(rows);
    
    Assert.assertEquals(1, results.size());
    
    // Calculate expected values
    double expectedSizeInMB = (10 * 1024.0 + 20 * 1024.0 + 5 * 1024.0 * 1024.0) / (1024.0 * 1024.0);
    double expectedTimeInSec = (500 * 1_000_000.0 + 300 * 1_000_000.0 + 1.2 * 1_000_000_000.0) / 1_000_000_000.0;

    // Verify results
    Assert.assertEquals(expectedSizeInMB, results.get(0).getValue("total_size_mb"));
    Assert.assertEquals(expectedTimeInSec, results.get(0).getValue("total_time_sec"));
  }

  @Test
  public void testAverageAggregation() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_size :time_taken avg_size_kb avg_time_ms 'average' 'KB' 'ms'"
    };

    List<Row> rows = new ArrayList<>();
    rows.add(createRow("2MB", "100ms", "row1"));
    rows.add(createRow("1MB", "200ms", "row2"));
    rows.add(createRow("3MB", "300ms", "row3"));

    RecipePipeline executor = TestingRig.execute(recipe);
    List<Row> results = executor.execute(rows);
    
    Assert.assertEquals(1, results.size());
    
    // Calculate expected values
    double totalSizeInKB = (2 * 1024.0 * 1024.0 + 1 * 1024.0 * 1024.0 + 3 * 1024.0 * 1024.0) / 1024.0;
    double totalTimeInMS = (100 * 1_000_000.0 + 200 * 1_000_000.0 + 300 * 1_000_000.0) / 1_000_000.0;
    double expectedAvgSizeInKB = totalSizeInKB / 3;
    double expectedAvgTimeInMS = totalTimeInMS / 3;

    // Verify results
    Assert.assertEquals(expectedAvgSizeInKB, results.get(0).getValue("avg_size_kb"));
    Assert.assertEquals(expectedAvgTimeInMS, results.get(0).getValue("avg_time_ms"));
  }

  @Test
  public void testDifferentOutputUnits() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_size :time_taken total_size_gb total_time_min 'total' 'GB' 'min'"
    };

    List<Row> rows = new ArrayList<>();
    rows.add(createRow("1GB", "30s", "row1"));
    rows.add(createRow("2GB", "90s", "row2"));

    RecipePipeline executor = TestingRig.execute(recipe);
    List<Row> results = executor.execute(rows);
    
    Assert.assertEquals(1, results.size());
    
    // Calculate expected values
    double expectedSizeInGB = (1.0 + 2.0);
    double expectedTimeInMin = (30.0 + 90.0) / 60.0;

    // Verify results
    Assert.assertEquals(expectedSizeInGB, results.get(0).getValue("total_size_gb"));
    Assert.assertEquals(expectedTimeInMin, results.get(0).getValue("total_time_min"));
  }

  @Test(expected = DirectiveParseException.class)
  public void testInvalidSizeUnit() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_size :time_taken total_size total_time 'total' 'PB' 's'"
    };
    
    TestingRig.execute(recipe);
  }
  @Test(expected = DirectiveParseException.class)
  public void testInvalidTimeUnit() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_size :time_taken total_size total_time 'total' 'MB' 'days'"
    };
    
    TestingRig.execute(recipe);
  }

  @Test(expected = DirectiveParseException.class)
  public void testInvalidAggregationType() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_size :time_taken total_size total_time 'maximum' 'MB' 's'"
    };
    
    TestingRig.execute(recipe);
  }

  @Test
  public void testMissingColumnsInSomeRows() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_size :time_taken total_size_mb total_time_sec"
    };

    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("data_size", "10MB");
    row1.add("time_taken", "1s");
    
    Row row2 = new Row();
    row2.add("data_size", "5MB");
    // missing time_taken column
    
    Row row3 = new Row();
    // missing data_size column
    row3.add("time_taken", "2s");
    
    rows.add(row1);
    rows.add(row2);
    rows.add(row3);

    RecipePipeline executor = TestingRig.execute(recipe);
    List<Row> results = executor.execute(rows);
    
    // Verify that rows with missing columns were skipped
    Assert.assertEquals(1, results.size());
    
    // Only row1 should have been processed
    Assert.assertEquals(10.0, results.get(0).getValue("total_size_mb"));
    Assert.assertEquals(1.0, results.get(0).getValue("total_time_sec"));
  }

  private Row createRow(String size, String time, String id) {
    Row row = new Row();
    row.add("data_transfer_size", size);
    row.add("data_size", size);
    row.add("response_time", time);
    row.add("time_taken", time);
    row.add("id", id);
    return row;
  }
}