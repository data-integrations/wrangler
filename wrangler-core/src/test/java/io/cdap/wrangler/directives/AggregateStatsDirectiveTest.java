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

package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AggregateStatsDirectiveTest {
    @Test
    public void testDefine() {
        AggregateStatsDirective directive = new AggregateStatsDirective();
        UsageDefinition definition = directive.define();
        
        Assert.assertEquals("aggregate-stats", definition.getDirectiveName());
        Assert.assertEquals(4, definition.getTokens().size());
        Assert.assertEquals(TokenType.COLUMN_NAME, definition.getTokens().get(0).type());
        Assert.assertEquals(TokenType.COLUMN_NAME, definition.getTokens().get(1).type());
        Assert.assertEquals(TokenType.COLUMN_NAME, definition.getTokens().get(2).type());
        Assert.assertEquals(TokenType.COLUMN_NAME, definition.getTokens().get(3).type());
    }

    @Test
    public void testSizeAndTimeCalculations() {
        AggregateStatsDirective directive = new AggregateStatsDirective();
        
        // Create test rows with various size and time units
        List<Row> rows = new ArrayList<>();
        
        // Row 1: 10MB and 100ms
        Row row1 = new Row();
        row1.add("data_transfer_size", "10MB");
        row1.add("response_time", "100ms");
        rows.add(row1);

        // Row 2: 5MB and 200ms
        Row row2 = new Row();
        row2.add("data_transfer_size", "5MB");
        row2.add("response_time", "200ms");
        rows.add(row2);

        // Row 3: 1GB and 1s
        Row row3 = new Row();
        row3.add("data_transfer_size", "1GB");
        row3.add("response_time", "1s");
        rows.add(row3);

        // Initialize directive
        directive.initialize(new Arguments() {
            @Override
            public <T extends Token> T value(String name) {
                switch (name) {
                    case "size-column":
                        return (T) new ColumnName("data_transfer_size");
                    case "time-column":
                        return (T) new ColumnName("response_time");
                    case "total-size-column":
                        return (T) new ColumnName("total_size_mb");
                    case "total-time-column":
                        return (T) new ColumnName("total_time_sec");
                    default:
                        return null;
                }
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
                return TokenType.COLUMN_NAME;
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
                return "";
            }

            @Override
            public com.google.gson.JsonElement toJson() {
                return null;
            }
        });

        // Execute directive
        List<Row> results = directive.execute(rows, null);

        // Verify results
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        // Expected calculations:
        // Size: 10MB + 5MB + 1GB = 10MB + 5MB + 1024MB = 1039MB
        Assert.assertEquals("1039.00 MB", result.getValue("total_size_mb"));
        
        // Time: 100ms + 200ms + 1s = 0.1s + 0.2s + 1s = 1.3s
        Assert.assertEquals("1.30 s", result.getValue("total_time_sec"));
    }

    @Test
    public void testMixedUnits() {
        AggregateStatsDirective directive = new AggregateStatsDirective();
        
        // Create test rows with mixed units
        List<Row> rows = new ArrayList<>();
        
        // Row 1: 1GB and 1s
        Row row1 = new Row();
        row1.add("data_transfer_size", "1GB");
        row1.add("response_time", "1s");
        rows.add(row1);

        // Row 2: 1024KB and 1000ms
        Row row2 = new Row();
        row2.add("data_transfer_size", "1024KB");
        row2.add("response_time", "1000ms");
        rows.add(row2);

        // Initialize directive
        directive.initialize(new Arguments() {
            @Override
            public <T extends Token> T value(String name) {
                switch (name) {
                    case "size-column":
                        return (T) new ColumnName("data_transfer_size");
                    case "time-column":
                        return (T) new ColumnName("response_time");
                    case "total-size-column":
                        return (T) new ColumnName("total_size_mb");
                    case "total-time-column":
                        return (T) new ColumnName("total_time_sec");
                    default:
                        return null;
                }
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
                return TokenType.COLUMN_NAME;
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
                return "";
            }

            @Override
            public com.google.gson.JsonElement toJson() {
                return null;
            }
        });

        // Execute directive
        List<Row> results = directive.execute(rows, null);

        // Verify results
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        // Expected calculations:
        // Size: 1GB + 1024KB = 1024MB + 1MB = 1025MB
        Assert.assertEquals("1025.00 MB", result.getValue("total_size_mb"));
        
        // Time: 1s + 1000ms = 1s + 1s = 2s
        Assert.assertEquals("2.00 s", result.getValue("total_time_sec"));
    }

    @Test
    public void testInvalidValues() {
        AggregateStatsDirective directive = new AggregateStatsDirective();
        
        // Create test rows with some invalid values
        List<Row> rows = new ArrayList<>();
        
        // Row 1: Valid values
        Row row1 = new Row();
        row1.add("data_transfer_size", "10MB");
        row1.add("response_time", "100ms");
        rows.add(row1);

        // Row 2: Invalid values
        Row row2 = new Row();
        row2.add("data_transfer_size", "invalid");
        row2.add("response_time", "invalid");
        rows.add(row2);

        // Initialize directive
        directive.initialize(new Arguments() {
            @Override
            public <T extends Token> T value(String name) {
                switch (name) {
                    case "size-column":
                        return (T) new ColumnName("data_transfer_size");
                    case "time-column":
                        return (T) new ColumnName("response_time");
                    case "total-size-column":
                        return (T) new ColumnName("total_size_mb");
                    case "total-time-column":
                        return (T) new ColumnName("total_time_sec");
                    default:
                        return null;
                }
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
                return TokenType.COLUMN_NAME;
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
                return "";
            }

            @Override
            public com.google.gson.JsonElement toJson() {
                return null;
            }
        });

        // Execute directive
        List<Row> results = directive.execute(rows, null);

        // Verify results - should only count valid values
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        // Only the valid values should be counted
        Assert.assertEquals("10.00 MB", result.getValue("total_size_mb"));
        Assert.assertEquals("0.10 s", result.getValue("total_time_sec"));
    }
}
 