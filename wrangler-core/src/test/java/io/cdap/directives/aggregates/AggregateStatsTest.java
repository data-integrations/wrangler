/*
 * Copyright © 2025 Cask Data, Inc.
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

// Reorder imports to follow checkstyle rules
import com.google.gson.JsonElement;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

public class AggregateStatsTest {

  @Test
  public void testAggregateStatsDirective() throws Exception {
    // Step 1: Prepare input rows
    List<Row> rows = new ArrayList<>();
    rows.add(new Row().add("data_transfer_size", "10KB").add("response_time", "2s"));
    rows.add(new Row().add("data_transfer_size", "5KB").add("response_time", "500ms"));

    // Step 2: Create an instance of the directive
    AggregateStats directive = new AggregateStats();

    // Step 3: Manually mock Arguments
    Arguments args = createMockArguments();

    // Step 4: Initialize and execute the directive
    directive.initialize(args);
    List<Row> result = directive.execute(rows, null);

    // Step 5: Validate output
    Assert.assertEquals(1, result.size());

    Row output = result.get(0);

    // Size: 10KB + 5KB = 15KB = 15 * 1024 bytes = 15360 bytes
    // MB = bytes / (1024 * 1024)
    double expectedMB = 15360.0 / (1024 * 1024);
    double actualMB = (Double) output.getValue("total_size_mb");

    // Time: 2s + 500ms = 2500ms = 2.5s
    double expectedSeconds = 2.5;
    double actualSeconds = (Double) output.getValue("total_time_sec");

    Assert.assertEquals(expectedMB, actualMB, 0.001);
    Assert.assertEquals(expectedSeconds, actualSeconds, 0.001);
  }

  @Test
  public void testEmptyInputRows() throws Exception {
    List<Row> rows = new ArrayList<>();

    AggregateStats directive = new AggregateStats();
    Arguments args = createMockArguments();

    directive.initialize(args);
    List<Row> result = directive.execute(rows, null);

    Assert.assertEquals(0, result.size());
  }

  @Test
  public void testInvalidData() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row().add("data_transfer_size", "invalidKB").add("response_time", "invalidTime"));

    AggregateStats directive = new AggregateStats();
    Arguments args = createMockArguments();

    directive.initialize(args);
    try {
      directive.execute(rows, null);
      Assert.fail("Expected an exception for invalid data");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Error aggregating stats"));
    }
  }

  @Test
  public void testLargeData() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row().add("data_transfer_size", "1024MB").add("response_time", "1h"));
    rows.add(new Row().add("data_transfer_size", "512MB").add("response_time", "30m"));

    AggregateStats directive = new AggregateStats();
    Arguments args = createMockArguments();

    directive.initialize(args);
    List<Row> result = directive.execute(rows, null);

    Assert.assertEquals(1, result.size());

    Row output = result.get(0);

    double expectedMB = 1536.0; // 1024MB + 512MB
    double actualMB = (Double) output.getValue("total_size_mb");

    double expectedSeconds = 5400.0; // 1h + 30m = 5400 seconds
    double actualSeconds = (Double) output.getValue("total_time_sec");

    Assert.assertEquals(expectedMB, actualMB, 0.001);
    Assert.assertEquals(expectedSeconds, actualSeconds, 0.001);
  }

  @Test
  public void testEdgeCases() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row().add("data_transfer_size", "0KB").add("response_time", "0s"));

    AggregateStats directive = new AggregateStats();
    Arguments args = createMockArguments();

    directive.initialize(args);
    List<Row> result = directive.execute(rows, null);

    Assert.assertEquals(1, result.size());

    Row output = result.get(0);

    double expectedMB = 0.0;
    double actualMB = (Double) output.getValue("total_size_mb");

    double expectedSeconds = 0.0;
    double actualSeconds = (Double) output.getValue("total_time_sec");

    Assert.assertEquals(expectedMB, actualMB, 0.001);
    Assert.assertEquals(expectedSeconds, actualSeconds, 0.001);
  }

  private Arguments createMockArguments() {
    return new Arguments() {
      @SuppressWarnings("unchecked")
      @Override
      public <T extends Token> T value(String name) {
        switch (name) {
          case "byteCol":
            return (T) new ColumnName("data_transfer_size");
          case "timeCol":
            return (T) new ColumnName("response_time");
          case "outputSizeCol":
            return (T) new Text("total_size_mb");
          case "outputTimeCol":
            return (T) new Text("total_time_sec");
        }
        return null;
      }

      @Override
      public int size() {
        return 4;
      }

      @Override
      public boolean contains(String name) {
        return true;
      }

      @Override
      public TokenType type(String name) {
        return null;
      }

      @Override
      public int line() {
        return 1;
      }

      @Override
      public int column() {
        return 0;
      }

      @Override
      public String source() {
        return "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec";
      }

      @Override
      public JsonElement toJson() {
        return null;
      }
    };
  }
}
