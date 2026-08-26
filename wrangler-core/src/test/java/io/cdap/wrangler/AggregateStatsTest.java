/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.wrangler.api.parser;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the {@link AggregateStats} directive.
 */
public class AggregateStatsTest {

  @Test
  public void testAggregateStats() throws Exception {
    // Create test data
    List<Row> rows = new ArrayList<>();
    
    // Row 1
    Row row1 = new Row();
    row1.add("data_transfer_size", "10KB");
    row1.add("response_time", "100ms");
    rows.add(row1);
    
    // Row 2
    Row row2 = new Row();
    row2.add("data_transfer_size", "5MB");
    row2.add("response_time", "2s");
    rows.add(row2);
    
    // Row 3
    Row row3 = new Row();
    row3.add("data_transfer_size", "1.5KB");
    row3.add("response_time", "50ms");
    rows.add(row3);
    
    // Define the recipe
    String[] directive = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
    };
    
    // Execute the recipe
    List<Row> results = TestingRig.execute(directive, rows);
    
    // Verify results
    Assert.assertEquals(1, results.size());
    
    // Calculate expected values
    // 10KB = 10 * 1024 bytes
    // 5MB = 5 * 1024 * 1024 bytes
    // 1.5KB = 1.5 * 1024 bytes
    // Total = 10 * 1024 + 5 * 1024 * 1024 + 1.5 * 1024 bytes
    // Convert to MB = (10 * 1024 + 5 * 1024 * 1024 + 1.5 * 1024) / (1024 * 1024) MB
    double expectedTotalSizeInMB = (10 * 1024 + 5 * 1024 * 1024 + 1.5 * 1024) / (1024.0 * 1024.0);
    
    // 100ms = 100 * 1_000_000 nanoseconds
    // 2s = 2 * 1_000_000_000 nanoseconds
    // 50ms = 50 * 1_000_000 nanoseconds
    // Total = 100 * 1_000_000 + 2 * 1_000_000_000 + 50 * 1_000_000 nanoseconds
    // Convert to seconds = (100 * 1_000_000 + 2 * 1_000_000_000 + 50 * 1_000_000) / 1_000_000_000 seconds
    double expectedTotalTimeInSeconds = (100 * 1_000_000.0 + 2 * 1_000_000_000.0 + 50 * 1_000_000.0) / 1_000_000_000.0;
    
    // Check values with tolerance for floating-point comparison
    Assert.assertEquals(expectedTotalSizeInMB, (double) results.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(expectedTotalTimeInSeconds, (double) results.get(0).getValue("total_time_sec"), 0.001);
  }

  @Test
  public void testAggregateStatsWithCustomUnits() throws Exception {
    // Create test data
    List<Row> rows = new ArrayList<>();
    
    // Row 1
    Row row1 = new Row();
    row1.add("size", "10KB");
    row1.add("time", "100ms");
    rows.add(row1);
    
    // Row 2
    Row row2 = new Row();
    row2.add("size", "5MB");
    row2.add("time", "2s");
    rows.add(row2);
    
    // Define the recipe with custom output units
    String[] directive = new String[] {
      "aggregate-stats :size :time :total_size :total_time GB m"
    };
    
    // Execute the recipe
    List<Row> results = TestingRig.execute(directive, rows);
    
    // Verify results
    Assert.assertEquals(1, results.size());
    
    // Calculate expected values
    // Total bytes = 10 * 1024 + 5 * 1024 * 1024 bytes
    // Convert to GB = (10 * 1024 + 5 * 1024 * 1024) / (1024 * 1024 * 1024) GB
    double expectedTotalSizeInGB = (10 * 1024.0 + 5 * 1024 * 1024.0) / (1024.0 * 1024 * 1024);
    
    // Total nanoseconds = 100 * 1_000_000 + 2 * 1_000_000_000 nanoseconds
    // Convert to minutes = (100 * 1_000_000 + 2 * 1_000_000_000) / (60 * 1_000_000_000) minutes
    double expectedTotalTimeInMinutes = (100 * 1_000_000.0 + 2 * 1_000_000_000.0) / (60.0 * 1_000_000_000);
    
    // Check values with tolerance for floating-point comparison
    Assert.assertEquals(expectedTotalSizeInGB, (double) results.get(0).getValue("total_size"), 0.001);
    Assert.assertEquals(expectedTotalTimeInMinutes, (double) results.get(0).getValue("total_time"), 0.001);
  }

  @Test
  public void testAggregateStatsWithMixedTypes() throws Exception {
    // Create test data with mixed types
    List<Row> rows = new ArrayList<>();
    
    // Row with string representations
    Row row1 = new Row();
    row1.add("size", "10KB");
    row1.add("time", "100ms");
    rows.add(row1);
    
    // Row with numeric values (raw bytes and nanoseconds)
    Row row2 = new Row();
    row2.add("size", 5242880L); // 5MB in bytes
    row2.add("time", 2000000000L); // 2s in nanoseconds
    rows.add(row2);
    
    // Define the recipe
    String[] directive = new String[] {
      "aggregate-stats :size :time :total_size_mb :total_time_sec"
    };
    
    // Execute the recipe
    List<Row> results = TestingRig.execute(directive, rows);
    
    // Verify results
    Assert.assertEquals(1, results.size());
    
    // Calculate expected values
    double expectedTotalSizeInMB = (10 * 1024.0 + 5242880) / (1024.0 * 1024.0);
    double expectedTotalTimeInSeconds = (100 * 1_000_000.0 + 2000000000.0) / 1_000_000_000.0;
    
    // Check values with tolerance for floating-point comparison
    Assert.assertEquals(expectedTotalSizeInMB, (double) results.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(expectedTotalTimeInSeconds, (double) results.get(0).getValue("total_time_sec"), 0.001);
  }

  @Test
  public void testEmptyInput() throws Exception {
    List<Row> rows = new ArrayList<>();
    
    String[] directive = new String[] {
      "aggregate-stats :size :time :total_size :total_time"
    };
    
    List<Row> results = TestingRig.execute(directive, rows);
    
    // Should return empty result for empty input
    Assert.assertEquals(0, results.size());
  }

  @Test(expected = Exception.class)
  public void testInvalidSizeUnit() throws Exception {
    List<Row> rows = new ArrayList<>();
    Row row = new Row();
    row.add("size", "10KB");
    row.add("time", "100ms");
    rows.add(row);
    
    String[] directive = new String[] {
      "aggregate-stats :size :time :total_size :total_time XB s"
    };
    
    TestingRig.execute(directive, rows);
  }

  @Test(expected = Exception.class)
  public void testInvalidTimeUnit() throws Exception {
    List<Row> rows = new ArrayList<>();
    Row row = new Row();
    row.add("size", "10KB");
    row.add("time", "100ms");
    rows.add(row);
    
    String[] directive = new String[] {
      "aggregate-stats :size :time :total_size :total_time MB xs"
    };
    
    TestingRig.execute(directive, rows);
  }
}
