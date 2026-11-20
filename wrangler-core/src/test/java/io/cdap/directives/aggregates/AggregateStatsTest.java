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
package io.cdap.directives.aggregates;

import com.google.gson.JsonElement;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

public class AggregateStatsTest {

    @Test
    public void testAggregateStatsDirective() throws Exception {
        // Prepare sample input rows with size in KB and response time in seconds/milliseconds
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("data_transfer_size", "12KB").add("response_time", "1.2s"));
        rows.add(new Row().add("data_transfer_size", "8KB").add("response_time", "800ms"));

        // Instantiate the directive
        AggregateStats directive = new AggregateStats();

        // Mock the arguments to pass to the directive
        Arguments mockArgs = createMockArguments();

        // Initialize and execute the directive
        directive.initialize(mockArgs);
        List<Row> result = directive.execute(rows, null);

        // Validate output row count
        Assert.assertEquals(1, result.size());
        Row output = result.get(0);

        // Expected: 20KB = 20480 bytes → 20480 / (1024 * 1024)
        double expectedMB = 20480.0 / (1024 * 1024);
        double actualMB = (Double) output.getValue("total_size_mb");

        // Expected time: 1.2s + 0.8s = 2.0s
        double expectedSeconds = 2.0;
        double actualSeconds = (Double) output.getValue("total_time_sec");

        Assert.assertEquals(expectedMB, actualMB, 0.001);
        Assert.assertEquals(expectedSeconds, actualSeconds, 0.001);
    }

    @Test
    public void testEmptyInputRows() throws Exception {
        // Test case with no input rows
        List<Row> rows = new ArrayList<>();
        AggregateStats directive = new AggregateStats();
        Arguments mockArgs = createMockArguments();

        directive.initialize(mockArgs);
        List<Row> result = directive.execute(rows, null);

        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testInvalidData() throws Exception {
        // Input rows with invalid formats
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("data_transfer_size", "abcKB").add("response_time", "xyzTime"));

        AggregateStats directive = new AggregateStats();
        Arguments mockArgs = createMockArguments();

        directive.initialize(mockArgs);
        try {
            directive.execute(rows, null);
            Assert.fail("Expected an exception due to invalid data format");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Error aggregating stats"));
        }
    }

    @Test
    public void testLargeData() throws Exception {
        // Test with large values in MB and time in hours/minutes
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("data_transfer_size", "2048MB").add("response_time", "1h"));
        rows.add(new Row().add("data_transfer_size", "1024MB").add("response_time", "30m"));

        AggregateStats directive = new AggregateStats();
        Arguments mockArgs = createMockArguments();

        directive.initialize(mockArgs);
        List<Row> result = directive.execute(rows, null);

        Assert.assertEquals(1, result.size());

        Row output = result.get(0);

        double expectedMB = 3072.0; // 2048MB + 1024MB
        double actualMB = (Double) output.getValue("total_size_mb");

        double expectedSeconds = 5400.0; // 1h + 30m = 3600 + 1800 = 5400s
        double actualSeconds = (Double) output.getValue("total_time_sec");

        Assert.assertEquals(expectedMB, actualMB, 0.001);
        Assert.assertEquals(expectedSeconds, actualSeconds, 0.001);
    }

    @Test
    public void testEdgeCases() throws Exception {
        // Edge case: zero data and zero time
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("data_transfer_size", "0KB").add("response_time", "0s"));

        AggregateStats directive = new AggregateStats();
        Arguments mockArgs = createMockArguments();

        directive.initialize(mockArgs);
        List<Row> result = directive.execute(rows, null);

        Assert.assertEquals(1, result.size());

        Row output = result.get(0);

        double actualMB = (Double) output.getValue("total_size_mb");
        double actualSeconds = (Double) output.getValue("total_time_sec");

        Assert.assertEquals(0.0, actualMB, 0.001);
        Assert.assertEquals(0.0, actualSeconds, 0.001);
    }

    // Create mock implementation of Arguments for unit testing
    private Arguments createMockArguments() {
        return new Arguments() {
            @SuppressWarnings("unchecked")
            @Override
            public <T extends Token> T value(String name) {
                switch (name) {
                    case "byteCol": return (T) new ColumnName("data_transfer_size");
                    case "timeCol": return (T) new ColumnName("response_time");
                    case "outputSizeCol": return (T) new Text("total_size_mb");
                    case "outputTimeCol": return (T) new Text("total_time_sec");
                }
                return null;
            }

            @Override public int size() { 
                return 4; 
            }

            @Override public boolean contains(String name) { 
                return true; 
            }

            @Override public TokenType type(String name) { 
                return null; 
            }

            @Override public int line() { 
                return 1; 
            }

            @Override public int column() { 
                return 0; 
            }

            @Override public String source() {
                return "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec";
            }
            @Override public JsonElement toJson() { 
                return null; 
            }
        };
    }
}
