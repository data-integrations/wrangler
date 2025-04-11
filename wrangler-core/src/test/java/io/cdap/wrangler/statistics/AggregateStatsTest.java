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

package io.cdap.wrangler.statistics;

import io.cdap.directives.aggregates.AggregateStats;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {

    @Test
    public void testBasicAggregation() throws Exception {
        ByteSize size1 = new ByteSize("1MB");
        ByteSize size2 = new ByteSize("2MB");
        TimeDuration time1 = new TimeDuration("1s");
        TimeDuration time2 = new TimeDuration("2s");

        Row row1 = new Row();
        row1.add("size", size1);
        row1.add("time", time1);

        Row row2 = new Row();
        row2.add("size", size2);
        row2.add("time", time2);

        AggregateStats directive = new AggregateStats(
                new UsageDefinition("aggregate-stats", "size", "time", "total_size", "avg_size", "total_time",
                        "avg_time"));

        List<Row> rows = Arrays.asList(row1, row2);
        rows = directive.execute(rows, null);

        Assert.assertEquals(2, rows.size());

        // Check total size (3MB)
        Assert.assertEquals(3.0, rows.get(0).getValue("total_size"));
        // Check average size (1.5MB)
        Assert.assertEquals(1.5, rows.get(0).getValue("avg_size"));
        // Check total time (3s)
        Assert.assertEquals(3.0, rows.get(0).getValue("total_time"));
        // Check average time (1.5s)
        Assert.assertEquals(1.5, rows.get(0).getValue("avg_time"));
    }

    @Test
    public void testMixedUnitsAggregation() throws Exception {
        ByteSize size1 = new ByteSize("1KB");
        ByteSize size2 = new ByteSize("1MB");
        TimeDuration time1 = new TimeDuration("1s");
        TimeDuration time2 = new TimeDuration("1m");

        Row row1 = new Row();
        row1.add("size", size1);
        row1.add("time", time1);

        Row row2 = new Row();
        row2.add("size", size2);
        row2.add("time", time2);

        AggregateStats directive = new AggregateStats(
                new UsageDefinition("aggregate-stats", "size", "time", "total_size", "avg_size", "total_time",
                        "avg_time"));

        List<Row> rows = Arrays.asList(row1, row2);
        rows = directive.execute(rows, null);

        Assert.assertEquals(2, rows.size());

        // 1KB + 1MB = 1025KB = 1.001953125MB
        Assert.assertEquals(1.001953125, rows.get(0).getValue("total_size"), 0.0001);
        Assert.assertEquals(0.5009765625, rows.get(0).getValue("avg_size"), 0.0001);

        // 1s + 60s = 61s
        Assert.assertEquals(61.0, rows.get(0).getValue("total_time"), 0.0001);
        Assert.assertEquals(30.5, rows.get(0).getValue("avg_time"), 0.0001);
    }

    @Test
    public void testFullRecipeWithJsonData() throws Exception {
        String[] directives = new String[] {
                "parse-as-json :records",
                "parse-as-byte-size :file_size file_size_bytes",
                "parse-as-time-duration :response_time response_time_ms",
                "aggregate-stats :file_size_bytes :response_time_ms total_size avg_size total_time avg_time"
        };

        // Create test data similar to test-data.json
        Row row1 = new Row();
        row1.add("records", "{\"file_size\": \"1.5KB\", \"response_time\": \"250ms\", \"name\": \"file1.txt\"}");

        Row row2 = new Row();
        row2.add("records", "{\"file_size\": \"2.8MB\", \"response_time\": \"1.2s\", \"name\": \"file2.jpg\"}");

        Row row3 = new Row();
        row3.add("records", "{\"file_size\": \"512B\", \"response_time\": \"450ms\", \"name\": \"file3.txt\"}");

        Row row4 = new Row();
        row4.add("records", "{\"file_size\": \"15MB\", \"response_time\": \"3.5s\", \"name\": \"file4.mp4\"}");

        Row row5 = new Row();
        row5.add("records", "{\"file_size\": \"750KB\", \"response_time\": \"180ms\", \"name\": \"file5.pdf\"}");

        List<Row> rows = TestingRig.execute(directives, Arrays.asList(row1, row2, row3, row4, row5));

        Assert.assertEquals(5, rows.size());

        // Verify the final aggregated metrics (checking the first row's values)
        Row firstRow = rows.get(0);
        Assert.assertTrue(firstRow.getValue("total_size") instanceof Double);
        Assert.assertTrue(firstRow.getValue("avg_size") instanceof Double);
        Assert.assertTrue(firstRow.getValue("total_time") instanceof Double);
        Assert.assertTrue(firstRow.getValue("avg_time") instanceof Double);

        // Expected values (approximate):
        // Total size: 1.5KB + 2.8MB + 512B + 15MB + 750KB ≈ 18.55MB
        // Avg size: 18.55MB / 5 ≈ 3.71MB
        // Total time: 250ms + 1.2s + 450ms + 3.5s + 180ms = 5.58s
        // Avg time: 5.58s / 5 = 1.116s

        // Check with some tolerance for floating-point calculations
        Assert.assertTrue(firstRow.getValue("total_size").toString().startsWith("18."));
        Assert.assertTrue(firstRow.getValue("avg_size").toString().startsWith("3.7"));
        Assert.assertTrue(Math.abs(5.58 - (Double) firstRow.getValue("total_time")) < 0.1);
        Assert.assertTrue(Math.abs(1.116 - (Double) firstRow.getValue("avg_time")) < 0.1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSizeInput() throws Exception {
        ByteSize size1 = new ByteSize("1MB");
        Row row1 = new Row();
        row1.add("size", size1);
        row1.add("time", "not a time duration");

        AggregateStats directive = new AggregateStats(
                new UsageDefinition("aggregate-stats", "size", "time", "total_size", "avg_size", "total_time",
                        "avg_time"));

        directive.execute(Arrays.asList(row1), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDurationInput() throws Exception {
        TimeDuration time1 = new TimeDuration("1s");
        Row row1 = new Row();
        row1.add("size", "not a byte size");
        row1.add("time", time1);

        AggregateStats directive = new AggregateStats(
                new UsageDefinition("aggregate-stats", "size", "time", "total_size", "avg_size", "total_time",
                        "avg_time"));

        directive.execute(Arrays.asList(row1), null);
    }
}
