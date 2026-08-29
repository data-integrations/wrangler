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

package io.cdap.directives.aggregates;

import io.cdap.wrangler.TestingPipelineContext;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.parser.MapArguments;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link SizeTimeAggregator}.
 */
public class SizeTimeAggregatorTest {

    @Test
    public void testUsageDefinition() {
        SizeTimeAggregator directive = new SizeTimeAggregator();
        UsageDefinition definition = directive.define();
        Assert.assertNotNull(definition);
        Assert.assertEquals(7, definition.getTokens().size());
        Assert.assertEquals(TokenType.COLUMN_NAME, definition.getTokens().get(0).type());
        Assert.assertEquals(TokenType.COLUMN_NAME, definition.getTokens().get(1).type());
        Assert.assertEquals(TokenType.COLUMN_NAME, definition.getTokens().get(2).type());
        Assert.assertEquals(TokenType.COLUMN_NAME, definition.getTokens().get(3).type());
        Assert.assertEquals(TokenType.TEXT, definition.getTokens().get(4).type());
        Assert.assertEquals(TokenType.TEXT, definition.getTokens().get(5).type());
        Assert.assertEquals(TokenType.TEXT, definition.getTokens().get(6).type());
    }

    @Test
    public void testByteSizeAggregation() throws Exception {
        SizeTimeAggregator directive = new SizeTimeAggregator();

        // Set up arguments
        Map<String, Object> args = new HashMap<>();
        args.put("size-column", new ColumnName("size"));
        args.put("time-column", new ColumnName("time"));
        args.put("target-size-column", new ColumnName("total_size"));
        args.put("target-time-column", new ColumnName("total_time"));

        // Initialize the directive manually
        directive.initialize(new DirectiveArgumentsTest(args));

        // Create some test data with various byte sizes
        List<Row> rows = Arrays.asList(
                new Row("size", "1KB").add("time", "5s"),
                new Row("size", "500B").add("time", "10s"),
                new Row("size", "2MB").add("time", "15s"));

        // Create a test execution context
        ExecutorContext context = new TestingPipelineContext();

        // Execute the directive
        directive.execute(rows, context);

        // Get the aggregation result
        Row result = directive.getAggregationResult(context);

        // The total should be 1KB + 500B + 2MB = approximately 2MB + 1KB + 500B
        // 1KB = 1024 bytes, 2MB = 2097152 bytes, 500B = 500 bytes
        // Total = 2098676 bytes
        Assert.assertEquals(2098676.0, ((Number) result.getValue("total_size")).doubleValue(), 0.0001);

        // The time total should be 5s + 10s + 15s = 30s = 30000ms
        Assert.assertEquals(30000.0, ((Number) result.getValue("total_time")).doubleValue(), 0.0001);
    }

    @Test
    public void testByteSizeAggregationWithUnits() throws Exception {
        SizeTimeAggregator directive = new SizeTimeAggregator();

        // Set up arguments with specific units
        Map<String, Object> args = new HashMap<>();
        args.put("size-column", new ColumnName("size"));
        args.put("time-column", new ColumnName("time"));
        args.put("target-size-column", new ColumnName("total_size"));
        args.put("target-time-column", new ColumnName("total_time"));
        args.put("size-unit", new Text("MB"));
        args.put("time-unit", new Text("s"));

        // Initialize the directive manually
        directive.initialize(new DirectiveArgumentsTest(args));

        // Create some test data
        List<Row> rows = Arrays.asList(
                new Row("size", "1KB").add("time", "5s"),
                new Row("size", "500B").add("time", "10s"),
                new Row("size", "2MB").add("time", "15s"));

        // Create a test execution context
        ExecutorContext context = new TestingPipelineContext();

        // Execute the directive
        directive.execute(rows, context);

        // Get the aggregation result
        Row result = directive.getAggregationResult(context);

        // Total is 2098676 bytes = 2.00151 MB (approximately)
        double expectedMB = 2098676.0 / (1024.0 * 1024.0);
        Assert.assertEquals(expectedMB, ((Number) result.getValue("total_size")).doubleValue(), 0.0001);

        // The time total in seconds should be 30s
        Assert.assertEquals(30.0, ((Number) result.getValue("total_time")).doubleValue(), 0.0001);
    }

    @Test
    public void testAverageAggregation() throws Exception {
        SizeTimeAggregator directive = new SizeTimeAggregator();

        // Set up arguments for average aggregation
        Map<String, Object> args = new HashMap<>();
        args.put("size-column", new ColumnName("size"));
        args.put("time-column", new ColumnName("time"));
        args.put("target-size-column", new ColumnName("avg_size"));
        args.put("target-time-column", new ColumnName("avg_time"));
        args.put("aggregate-type", new Text("average"));
        args.put("size-unit", new Text("KB"));
        args.put("time-unit", new Text("s"));

        // Initialize the directive manually
        directive.initialize(new DirectiveArgumentsTest(args));

        // Create some test data
        List<Row> rows = Arrays.asList(
                new Row("size", "1KB").add("time", "5s"),
                new Row("size", "500B").add("time", "10s"),
                new Row("size", "2MB").add("time", "15s"));

        // Create a test execution context
        ExecutorContext context = new TestingPipelineContext();

        // Execute the directive
        directive.execute(rows, context);

        // Get the aggregation result
        Row result = directive.getAggregationResult(context);

        // Total is 2098676 bytes = 2050.46 KB, average is 2050.46 / 3 = 683.49 KB
        double expectedKB = 2098676.0 / 1024.0 / 3.0;
        Assert.assertEquals(expectedKB, ((Number) result.getValue("avg_size")).doubleValue(), 0.01);

        // The average time in seconds should be 30s / 3 = 10s
        Assert.assertEquals(10.0, ((Number) result.getValue("avg_time")).doubleValue(), 0.0001);
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidSizeUnit() throws Exception {
        SizeTimeAggregator directive = new SizeTimeAggregator();

        // Set up arguments with an invalid size unit
        Map<String, Object> args = new HashMap<>();
        args.put("size-column", new ColumnName("size"));
        args.put("time-column", new ColumnName("time"));
        args.put("target-size-column", new ColumnName("total_size"));
        args.put("target-time-column", new ColumnName("total_time"));
        args.put("size-unit", new Text("XB")); // Invalid unit

        // Initialize the directive manually - should throw an exception
        directive.initialize(new DirectiveArgumentsTest(args));
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidTimeUnit() throws Exception {
        SizeTimeAggregator directive = new SizeTimeAggregator();

        // Set up arguments with an invalid time unit
        Map<String, Object> args = new HashMap<>();
        args.put("size-column", new ColumnName("size"));
        args.put("time-column", new ColumnName("time"));
        args.put("target-size-column", new ColumnName("total_size"));
        args.put("target-time-column", new ColumnName("total_time"));
        args.put("time-unit", new Text("x")); // Invalid unit

        // Initialize the directive manually - should throw an exception
        directive.initialize(new DirectiveArgumentsTest(args));
    }

    @Test
    public void testWithMixedDataTypes() throws Exception {
        SizeTimeAggregator directive = new SizeTimeAggregator();

        // Set up arguments
        Map<String, Object> args = new HashMap<>();
        args.put("size-column", new ColumnName("size"));
        args.put("time-column", new ColumnName("time"));
        args.put("target-size-column", new ColumnName("total_size"));
        args.put("target-time-column", new ColumnName("total_time"));

        // Initialize the directive manually
        directive.initialize(new DirectiveArgumentsTest(args));

        // Create some test data with various formats and some invalid entries
        List<Row> rows = Arrays.asList(
                new Row("size", "1KB").add("time", "5s"),
                new Row("size", "not-a-size").add("time", "10s"), // Invalid size
                new Row("size", "2MB").add("time", "not-a-time"), // Invalid time
                new Row("size", "3MB").add("time", "15s"),
                new Row("other", "value") // Missing columns
        );

        // Create a test execution context
        ExecutorContext context = new TestingPipelineContext();

        // Execute the directive
        directive.execute(rows, context);

        // Get the aggregation result - only valid entries should be counted
        Row result = directive.getAggregationResult(context);

        // The total should be 1KB + 3MB = approximately 3MB + 1KB
        // 1KB = 1024 bytes, 3MB = 3145728 bytes
        // Total = 3146752 bytes
        Assert.assertEquals(3146752.0, ((Number) result.getValue("total_size")).doubleValue(), 0.0001);

        // The time total should be 5s + 15s = 20s = 20000ms
        Assert.assertEquals(20000.0, ((Number) result.getValue("total_time")).doubleValue(), 0.0001);
    }

    /**
     * Simple implementation of Arguments for testing.
     */
    private static class DirectiveArgumentsTest implements io.cdap.wrangler.api.Arguments {
        private final Map<String, Object> tokens;

        public DirectiveArgumentsTest(Map<String, Object> args) {
            this.tokens = args;
        }

        @Override
        public <T extends io.cdap.wrangler.api.parser.Token> T value(String name) {
            return (T) tokens.get(name);
        }

        @Override
        public int size() {
            return tokens.size();
        }

        @Override
        public boolean contains(String name) {
            return tokens.containsKey(name);
        }

        @Override
        public io.cdap.wrangler.api.parser.TokenType type(String name) {
            if (tokens.get(name) instanceof io.cdap.wrangler.api.parser.Token) {
                return ((io.cdap.wrangler.api.parser.Token) tokens.get(name)).type();
            }
            return null;
        }

        @Override
        public int line() {
            return 0;
        }

        @Override
        public int column() {
            return 0;
        }

        @Override
        public String source() {
            return "Test source";
        }

        @Override
        public com.google.gson.JsonElement toJson() {
            return new com.google.gson.JsonObject();
        }
    }
}