/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file exce    Row row3 = new Row();
    row3.add("data_transfer_size", "3072KB");
    row3.add(    Row row1 = new Row();
    row1.add("data_transfer_size", "0.5GB");
    row1.add("response_time", "0.5s");
    rows.add(row1);

    Row row2 = new Row();
    row2.add("data_transfer_size", "1.5GB");
    row2.add("response_time", "1.5s");
    rows.add(row2);

    // Execute the directive
    @SuppressWarnings("unchecked")
    RecipePipeline<Row, Row, Row> pipeline = (RecipePipeline<Row, Row, Row>) TestingRig.execute(recipe);
    List<Row> results = pipeline.execute(rows);e", "300ms");
    rows.add(row3);

    // Execute the directive
    @SuppressWarnings("unchecked")
    RecipePipeline<Row, Row, Row> pipeline = (RecipePipeline<Row, Row, Row>) TestingRig.execute(recipe);
    List<Row> results = pipeline.execute(rows);mpliance w    // E    // Expected max time: 150ms
    Assert.assertEquals(150.0, ((Double) result.getValue("max_time_ms")).doubleValue(), 0.001);
  }

  @Test
  public void testAggregateTotalInGB() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_gb :total_time_s 'GB' 's' 'total'"
    };ax time: 150ms
    Assert.assertEquals(150.0, ((Double) result.getValue("max_time_ms")).doubleValue(), 0.001);
  }

  @Test
  public void testAggregateTotalInGB() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_gb :total_time_s 'GB' 's' 'total'"
    };icense. You may obtain a copy of
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

import io.cdap.wrangler.TestingPipelineContext;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link AggregateStats} directive.
 */
public class AggregateStatsTest {

  @Test
  public void testAggregateTotal() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec 'MB' 's' 'total'"
    };

    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("data_transfer_size", "2MB");
    row1.add("response_time", "100ms");
    rows.add(row1);

    Row row2 = new Row();
    row2.add("data_transfer_size", "3MB");
    row2.add("response_time", "150ms");
    rows.add(row2);

    Row row3 = new Row();
    row3.add("data_transfer_size", "5MB");
    row3.add("response_time", "250ms");
    rows.add(row3);

    // Execute the directive
    RecipePipeline<Row, Row, Row> pipeline = TestingRig.execute(recipe, rows);
    List<Row> results = pipeline.execute(rows);

    // Verify results
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);

    // Expected total size: 2MB + 3MB + 5MB = 10MB
    Assert.assertEquals(10.0, ((Double) result.getValue("total_size_mb")).doubleValue(), 0.001);

    // Expected total time: 100ms + 150ms + 250ms = 500ms = 0.5s
    Assert.assertEquals(0.5, ((Double) result.getValue("total_time_sec")).doubleValue(), 0.001);
  }

  @Test
  public void testAggregateAverage() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :avg_size_kb :avg_time_ms 'KB' 'ms' 'average'"
    };

    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("data_transfer_size", "1024KB");
    row1.add("response_time", "100ms");
    rows.add(row1);

    Row row2 = new Row();
    row2.add("data_transfer_size", "2048KB");
    row2.add("response_time", "200ms");
    rows.add(row2);

    Row row3 = new Row();
    row3.add("data_transfer_size", "3072KB");
    row3.add("response_time", "300ms");
    rows.add(row3);

    // Execute the directive
    @SuppressWarnings("unchecked")
    RecipePipeline<Row, Row, Row> pipeline = (RecipePipeline<Row, Row, Row>) TestingRig.execute(recipe);
    List<Row> results = pipeline.execute(rows);

    // Verify results
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);

    // Expected average size: (1024KB + 2048KB + 3072KB) / 3 = 2048KB
    Assert.assertEquals(2048.0, ((Double) result.getValue("avg_size_kb")).doubleValue(), 0.001);

    // Expected average time: (100ms + 200ms + 300ms) / 3 = 200ms
    Assert.assertEquals(200.0, ((Double) result.getValue("avg_time_ms")).doubleValue(), 0.001);
  }

  @Test
  public void testMinMaxAggregation() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :min_size_mb :min_time_ms 'MB' 'ms' 'min'"
    };

    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("data_transfer_size", "10MB");
    row1.add("response_time", "100ms");
    rows.add(row1);

    Row row2 = new Row();
    row2.add("data_transfer_size", "5MB");
    row2.add("response_time", "50ms");
    rows.add(row2);

    Row row3 = new Row();
    row3.add("data_transfer_size", "20MB");
    row3.add("response_time", "150ms");
    rows.add(row3);

    // Execute the directive
    RecipePipeline<Row, Row, Row> pipeline = TestingRig.execute(recipe, new TestingPipelineContext());
    List<Row> results = pipeline.execute(rows);

    // Verify results
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);

    // Expected min size: 5MB
    Assert.assertEquals(5.0, ((Double) result.getValue("min_size_mb")).doubleValue(), 0.001);

    // Expected min time: 50ms
    Assert.assertEquals(50.0, ((Double) result.getValue("min_time_ms")).doubleValue(), 0.001);

    // Test max aggregation
    recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :max_size_mb :max_time_ms 'MB' 'ms' 'max'"
    };

    // Suppressing unchecked conversion warning
    @SuppressWarnings("unchecked")
    RecipePipeline<Row, Row, Row> pipeline = (RecipePipeline<Row, Row, Row>) TestingRig.execute(recipe);
    List<Row> results = pipeline.execute(rows);

    // Verify results
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);

    // Expected max size: 20MB
    Assert.assertEquals(20.0, ((Double) result.getValue("max_size_mb")).doubleValue(), 0.001);

    // Expected max time: 150ms
    Assert.assertEquals(150.0, ((Double) result.getValue("max_time_ms")).doubleValue(), 0.001);
  }

  @Test
  public void testDifferentUnits() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_gb :total_time_s 'GB' 's' 'total'"
    };

    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("data_transfer_size", "1024MB");
    row1.add("response_time", "1000ms");
    rows.add(row1);

    Row row2 = new Row();
    row2.add("data_transfer_size", "1024MB");
    row2.add("response_time", "1000ms");
    rows.add(row2);

    // Execute the directive
    @SuppressWarnings("unchecked")
    RecipePipeline<Row, Row, Row> pipeline = (RecipePipeline<Row, Row, Row>) TestingRig.execute(recipe);
    List<Row> results = pipeline.execute(rows);

    // Verify results
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);

    // Expected total size: 1024MB + 1024MB = 2048MB = 2GB
    Assert.assertEquals(2.0, ((Double) result.getValue("total_size_gb")).doubleValue(), 0.001);

    // Expected total time: 1000ms + 1000ms = 2000ms = 2s
    Assert.assertEquals(2.0, ((Double) result.getValue("total_time_s")).doubleValue(), 0.001);
  }

  @Test
  public void testMixedUnitsInput() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_s 'MB' 's' 'total'"
    };

    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("data_transfer_size", "1GB");
    row1.add("response_time", "1s");
    rows.add(row1);

    Row row2 = new Row();
    row2.add("data_transfer_size", "1024MB");
    row2.add("response_time", "1000ms");
    rows.add(row2);

    // Execute the directive
    @SuppressWarnings("unchecked")
    RecipePipeline<Row, Row, Row> pipeline = (RecipePipeline<Row, Row, Row>) TestingRig.execute(recipe);
    List<Row> results = pipeline.execute(rows);

    // Verify results
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);

    // Expected total size: 1GB + 1024MB = 2048MB
    Assert.assertEquals(2048.0, ((Double) result.getValue("total_size_mb")).doubleValue(), 0.001);

    // Expected total time: 1s + 1000ms = 2s
    Assert.assertEquals(2.0, ((Double) result.getValue("total_time_s")).doubleValue(), 0.001);
  }

  @Test
  public void testHandleNullOrInvalidValues() throws Exception {
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_s 'MB' 's' 'total'"
    };

    List<Row> rows = new ArrayList<>();
    Row row1 = new Row();
    row1.add("data_transfer_size", "1GB");
    row1.add("response_time", "1s");
    rows.add(row1);

    Row row2 = new Row();
    row2.add("data_transfer_size", null);
    row2.add("response_time", "invalid");
    rows.add(row2);

    Row row3 = new Row();
    row3.add("data_transfer_size", "1024MB");
    row3.add("response_time", "1000ms");
    rows.add(row3);

    // Execute the directive
    @SuppressWarnings("unchecked")
    RecipePipeline<Row, Row, Row> pipeline = (RecipePipeline<Row, Row, Row>) TestingRig.execute(recipe);
    List<Row> results = pipeline.execute(rows);

    // Verify results
    Assert.assertEquals(1, results.size());
    Row result = results.get(0);

    // Expected total size: 1GB + 1024MB = 2048MB (ignoring null value)
    Assert.assertEquals(2048.0, ((Double) result.getValue("total_size_mb")).doubleValue(), 0.001);

    // Expected total time: 1s + 1000ms = 2s (ignoring invalid value)
    Assert.assertEquals(2.0, ((Double) result.getValue("total_time_s")).doubleValue(), 0.001);
  }
}