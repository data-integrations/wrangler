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
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for AggregateStats directive.
 */
public class AggregateStatsTest {

  // Helper to parse external CSV test data into List<Row>
  private List<Row> loadRowsFromCSV(String fileName) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
      getClass().getClassLoader().getResourceAsStream(fileName)))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(",");
        rows.add(new Row("data_transfer_size", parts[0]).add("response_time", parts[1]));
      }
    }
    return rows;
  }

  @Test
  public void testAggregateStatsWithVariousUnits() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("data_transfer_size", "100 MB").add("response_time", "500 ms"));
    rows.add(new Row("data_transfer_size", "1.5 GB").add("response_time", "1000 ms"));
    rows.add(new Row("data_transfer_size", "0.5 TB").add("response_time", "2 minutes"));
    rows.add(new Row("data_transfer_size", "500 kB").add("response_time", "300000 μs"));

    String[] recipe = new String[]{
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> results = TestingRig.execute(recipe, rows);

    Assert.assertEquals(1, results.size());

    double expectedSizeMB = (100)
        + (1.5 * 1024)
        + (0.5 * 1024 * 1024)
        + (500.0 / 1024);
    double expectedTimeSec = (500.0 / 1000)
        + (1000.0 / 1000)
        + (2 * 60)
        + (300000.0 / 1_000_000);

    Assert.assertEquals(expectedSizeMB, (double) results.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(expectedTimeSec, (double) results.get(0).getValue("total_time_sec"), 0.001);
  }

  @Test
  public void testMalformedInputs() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("data_transfer_size", "1XB").add("response_time", "500 ms")); // invalid unit
    rows.add(new Row("data_transfer_size", "100 MB").add("response_time", "abc ms")); // invalid time

    String[] recipe = new String[]{
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> results = TestingRig.execute(recipe, rows);

    Assert.assertEquals(1, results.size());

    double expectedSizeMB = 100;
    double expectedTimeSec = (500.0 / 1000);

    // Depending on your directive's logic — if it skips invalid values:
    Assert.assertEquals(expectedSizeMB, (double) results.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(expectedTimeSec, (double) results.get(0).getValue("total_time_sec"), 0.001);
  }

  @Test
  public void testAggregateStatsFromCSV() throws Exception {
    List<Row> rows = loadRowsFromCSV("aggregate_stats_test_data.csv");

    String[] recipe = new String[]{
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> results = TestingRig.execute(recipe, rows);

    Assert.assertEquals(1, results.size());

    // Assuming test CSV has known expected values
    double expectedSizeMB = 1624.5;
    double expectedTimeSec = 122.3;

    Assert.assertEquals(expectedSizeMB, (double) results.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(expectedTimeSec, (double) results.get(0).getValue("total_time_sec"), 0.001);
  }
}
