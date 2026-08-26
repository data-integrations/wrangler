/*
 * Copyright © 2017-2022 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link AggregateStats} directive.
 */
public class AggregateStatsTest {

  @Test
  public void testBasicAggregation() throws Exception {
    List<Row> rows = new ArrayList<>();
    
    // Add test data with ByteSize and TimeDuration values
    Row row1 = new Row();
    row1.add("data_transfer_size", new ByteSize("100KB"));
    row1.add("response_time", new TimeDuration("50ms"));
    rows.add(row1);
    
    Row row2 = new Row();
    row2.add("data_transfer_size", new ByteSize("200KB"));
    row2.add("response_time", new TimeDuration("75ms"));
    rows.add(row2);
    
    Row row3 = new Row();
    row3.add("data_transfer_size", new ByteSize("300KB"));
    row3.add("response_time", new TimeDuration("100ms"));
    rows.add(row3);
    
    String[] directive = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
    };
    
    List<Row> results = TestingRig.execute(directive, rows);
    
    // Find the row with actual values (the last one should have the aggregate values)
    Row aggregateRow = null;
    for (Row row : results) {
      Double value = (Double) row.getValue("total_size_mb");
      if (value != null && value > 0) {
        aggregateRow = row;
        break;
      }
    }
    Assert.assertNotNull("No row with aggregate values found", aggregateRow);
    
    // Total size should be 600KB = 0.6MB 
    double expectedSizeMB = 600.0 / 1024.0; // 600KB in MB
    Assert.assertEquals(expectedSizeMB, ((Double) aggregateRow.getValue("total_size_mb")).doubleValue(), 0.001);
    
    // Total time should be 225ms = 0.225s
    double expectedTimeSec = 225.0 / 1000.0; // 225ms in seconds
    Assert.assertEquals(expectedTimeSec, ((Double) aggregateRow.getValue("total_time_sec")).doubleValue(), 0.001);
  }
  
  @Test
  public void testAggregationWithDifferentUnits() throws Exception {
    List<Row> rows = new ArrayList<>();
    
    // Add test data with various units
    Row row1 = new Row();
    row1.add("data_transfer_size", new ByteSize("1MB"));
    row1.add("response_time", new TimeDuration("1s"));
    rows.add(row1);
    
    Row row2 = new Row();
    row2.add("data_transfer_size", new ByteSize("500KB"));
    row2.add("response_time", new TimeDuration("500ms"));
    rows.add(row2);
    
    Row row3 = new Row();
    row3.add("data_transfer_size", new ByteSize("2MB"));
    row3.add("response_time", new TimeDuration("2s"));
    rows.add(row3);
    
    String[] directive = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
    };
    
    List<Row> results = TestingRig.execute(directive, rows);
    
    // Find the row with actual values (the last one should have the aggregate values)
    Row aggregateRow = null;
    for (Row row : results) {
      Double value = (Double) row.getValue("total_size_mb");
      if (value != null && value > 0) {
        aggregateRow = row;
        break;
      }
    }
    Assert.assertNotNull("No row with aggregate values found", aggregateRow);
    
    // Total size should be 1MB + 500KB + 2MB = 3.5MB
    double expectedSizeMB = 3.5; // 3.5MB
    Assert.assertEquals(expectedSizeMB, ((Double) aggregateRow.getValue("total_size_mb")).doubleValue(), 0.01);
    
    // Total time should be 1s + 500ms + 2s = 3.5s
    double expectedTimeSec = 3.5; // 3.5 seconds
    Assert.assertEquals(expectedTimeSec, ((Double) aggregateRow.getValue("total_time_sec")).doubleValue(), 0.01);
  }
  
  @Test
  public void testAggregationWithSpecifiedOutputUnits() throws Exception {
    List<Row> rows = new ArrayList<>();
    
    // Add test data
    Row row1 = new Row();
    row1.add("data_transfer_size", new ByteSize("1GB"));
    row1.add("response_time", new TimeDuration("5m"));
    rows.add(row1);
    
    Row row2 = new Row();
    row2.add("data_transfer_size", new ByteSize("1GB"));
    row2.add("response_time", new TimeDuration("5m"));
    rows.add(row2);
    
    // Test with specified output units (GB and minutes)
    String[] directive = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_gb :total_time_min 'GB' 'm'"
    };
    
    List<Row> results = TestingRig.execute(directive, rows);
    
    // Find the row with actual values (the last one should have the aggregate values)
    Row aggregateRow = null;
    for (Row row : results) {
      Double value = (Double) row.getValue("total_size_gb");
      if (value != null && value > 0) {
        aggregateRow = row;
        break;
      }
    }
    Assert.assertNotNull("No row with aggregate values found", aggregateRow);
    
    // Total size should be 2GB
    Assert.assertEquals(2.0, ((Double) aggregateRow.getValue("total_size_gb")).doubleValue(), 0.001);
    
    // Total time should be 10 minutes
    Assert.assertEquals(10.0, ((Double) aggregateRow.getValue("total_time_min")).doubleValue(), 0.001);
  }
  
  @Test
  public void testAggregationWithStringInputs() throws Exception {
    List<Row> rows = new ArrayList<>();
    
    // Add test data as strings that can be parsed
    Row row1 = new Row();
    row1.add("data_transfer_size", "1MB");
    row1.add("response_time", "1s");
    rows.add(row1);
    
    Row row2 = new Row();
    row2.add("data_transfer_size", "1MB");
    row2.add("response_time", "1s");
    rows.add(row2);
    
    String[] directive = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
    };
    
    List<Row> results = TestingRig.execute(directive, rows);
    
    // Find the row with actual values (the last one should have the aggregate values)
    Row aggregateRow = null;
    for (Row row : results) {
      Double value = (Double) row.getValue("total_size_mb");
      if (value != null && value > 0) {
        aggregateRow = row;
        break;
      }
    }
    Assert.assertNotNull("No row with aggregate values found", aggregateRow);
    
    // Total size should be 2MB
    Assert.assertEquals(2.0, ((Double) aggregateRow.getValue("total_size_mb")).doubleValue(), 0.001);
    
    // Total time should be 2 seconds
    Assert.assertEquals(2.0, ((Double) aggregateRow.getValue("total_time_sec")).doubleValue(), 0.001);
  }
  
  @Test
  public void testEmptyInput() throws Exception {
    List<Row> rows = new ArrayList<>();
    
    // Create a single placeholder row since our directive will always generate
    // a result even for empty input
    rows.add(new Row());
    
    String[] directive = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
    };
    
    List<Row> results = TestingRig.execute(directive, rows);
    
    // Should have a single result row with zeros
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(0.0, ((Double) results.get(0).getValue("total_size_mb")).doubleValue(), 0.001);
    Assert.assertEquals(0.0, ((Double) results.get(0).getValue("total_time_sec")).doubleValue(), 0.001);
  }
  
  @Test
  public void testNullValues() throws Exception {
    List<Row> rows = new ArrayList<>();
    
    // Add test data with some null values
    Row row1 = new Row();
    row1.add("data_transfer_size", new ByteSize("1MB"));
    row1.add("response_time", null);
    rows.add(row1);
    
    Row row2 = new Row();
    row2.add("data_transfer_size", null);
    row2.add("response_time", new TimeDuration("1s"));
    rows.add(row2);
    
    Row row3 = new Row();
    row3.add("data_transfer_size", new ByteSize("1MB"));
    row3.add("response_time", new TimeDuration("1s"));
    rows.add(row3);
    
    String[] directive = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
    };
    
    List<Row> results = TestingRig.execute(directive, rows);
    
    // Find the row with actual values (the last one should have the aggregate values)
    Row aggregateRow = null;
    for (Row row : results) {
      Double value = (Double) row.getValue("total_size_mb");
      if (value != null && value > 0) {
        aggregateRow = row;
        break;
      }
    }
    Assert.assertNotNull("No row with aggregate values found", aggregateRow);
    
    // Total size should be 2MB (only the non-null values)
    Assert.assertEquals(2.0, ((Double) aggregateRow.getValue("total_size_mb")).doubleValue(), 0.001);
    
    // Total time should be 2 seconds (only the non-null values)
    Assert.assertEquals(2.0, ((Double) aggregateRow.getValue("total_time_sec")).doubleValue(), 0.001);
  }
} 
