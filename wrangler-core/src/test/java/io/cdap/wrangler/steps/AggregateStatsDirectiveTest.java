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

package io.cdap.wrangler.steps;

import io.cdap.directives.aggregates.AggregateStatsDirective;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link AggregateStatsDirective}
 */
public class AggregateStatsDirectiveTest {

  @Test
  public void testBasicAggregation() throws Exception {
    // Create sample data
    List<Row> rows = Arrays.asList(
      createRow("500KB", "750ms"),
      createRow("1.5MB", "2.5s"),
      createRow("250KB", "500ms")
    );

    // Define the recipe for total aggregation
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec MB s total"
    };

    // Expected results: 500KB + 1.5MB + 250KB = ~2.23MB, 750ms + 2.5s + 500ms = 3.75s
    List<Row> results = TestingRig.execute(recipe, rows);

    // Verify results
    assertEquals(1, results.size());
    Row result = results.get(0);

    // Verify total size (in MB)
    ByteSize totalSize = (ByteSize) result.getValue("total_size_mb");
    assertEquals(2.23, totalSize.getBytes() / (1024.0 * 1024.0), 0.01);

    // Verify total time (in seconds)
    TimeDuration totalTime = (TimeDuration) result.getValue("total_time_sec");
    assertEquals(3.75, totalTime.getNanoseconds() / 1_000_000_000.0, 0.01);
  }

  @Test
  public void testAverageAggregation() throws Exception {
    // Create sample data
    List<Row> rows = Arrays.asList(
      createRow("600KB", "900ms"),
      createRow("1.2MB", "1.8s"),
      createRow("300KB", "300ms")
    );

    // Define the recipe for average aggregation
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :avg_size_mb :avg_time_sec MB s average"
    };

    // Expected results: (600KB + 1.2MB + 300KB)/3 = ~0.7MB, (900ms + 1.8s + 300ms)/3 = 1s
    List<Row> results = TestingRig.execute(recipe, rows);

    // Verify results
    assertEquals(1, results.size());
    Row result = results.get(0);

    // Verify average size (in MB)
    ByteSize avgSize = (ByteSize) result.getValue("avg_size_mb");
    assertEquals(0.7, avgSize.getBytes() / (1024.0 * 1024.0), 0.01);

    // Verify average time (in seconds)
    TimeDuration avgTime = (TimeDuration) result.getValue("avg_time_sec");
    assertEquals(1.0, avgTime.getNanoseconds() / 1_000_000_000.0, 0.01);
  }

  @Test
  public void testCustomUnitConversion() throws Exception {
    List<Row> rows = Arrays.asList(
      createRow("1GB", "1h"),
      createRow("2GB", "2h")
    );

    // Define recipe with TB and minutes as output units
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_tb :total_time_m TB m total"
    };

    List<Row> results = TestingRig.execute(recipe, rows);
    assertEquals(1, results.size());
    Row result = results.get(0);

    // Verify total size (in TB): 3GB = 0.003TB
    ByteSize totalSize = (ByteSize) result.getValue("total_size_tb");
    assertEquals(0.003, totalSize.getBytes() / Math.pow(1024, 4), 0.001);

    // Verify total time (in minutes): 3h = 180m
    TimeDuration totalTime = (TimeDuration) result.getValue("total_time_m");
    assertEquals(180.0, totalTime.getNanoseconds() / (60.0 * 1_000_000_000L), 0.01);
  }

  @Test
  public void testEmptyData() throws Exception {
    List<Row> rows = Arrays.asList();

    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec MB s total"
    };

    List<Row> results = TestingRig.execute(recipe, rows);
    assertEquals(1, results.size());
    Row result = results.get(0);

    // Verify zero values
    ByteSize totalSize = (ByteSize) result.getValue("total_size_mb");
    assertEquals(0.0, totalSize.getBytes() / (1024.0 * 1024.0), 0.01);

    TimeDuration totalTime = (TimeDuration) result.getValue("total_time_sec");
    assertEquals(0.0, totalTime.getNanoseconds() / 1_000_000_000.0, 0.01);
  }

  private Row createRow(String size, String time) {
    Row row = new Row();
    row.add("data_transfer_size", new ByteSize(size));
    row.add("response_time", new TimeDuration(time));
    return row;
  }
}