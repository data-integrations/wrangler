// File: wrangler-core/src/test/java/io/cdap/wrangler/steps/AggregateStatsDirectiveTest.java

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

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.test.TestingRig;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link AggregateStatsDirective}
 */
public class AggregateStatsDirectiveTest {

    @Test
    public void testBasicAggregation() throws Exception {
        List<Row> rows = new ArrayList<>();
        
        // Create test rows
        Row row1 = new Row();
        row1.setValue("size", new ByteSize("100KB"));
        row1.setValue("time", new TimeDuration("500ms"));
        rows.add(row1);

        Row row2 = new Row();
        row2.setValue("size", new ByteSize("2MB"));
        row2.setValue("time", new TimeDuration("1.5s"));
        rows.add(row2);

        Row row3 = new Row();
        row3.setValue("size", new ByteSize("50KB"));
        row3.setValue("time", new TimeDuration("750ms"));
        rows.add(row3);

        // Create and execute directive
        String[] directive = new String[] {
            "aggregate-stats :size :time total_size_mb total_time_sec"
        };

        List<Row> results = TestingRig.execute(directive, rows);

        // Verify results
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);

        // Expected values:
        // Total size = (100 * 1024) + (2 * 1024 * 1024) + (50 * 1024) bytes = 2,199,552 bytes ≈ 2.097 MB
        // Total time = 500ms + 1500ms + 750ms = 2750ms = 2.75 seconds
        Assert.assertEquals(2.097, result.getValue("total_size_mb"), 0.001);
        Assert.assertEquals(2.75, result.getValue("total_time_sec"), 0.001);

        // Check averages
        Assert.assertEquals(0.699, result.getValue("total_size_mb_avg"), 0.001);
        Assert.assertEquals(0.917, result.getValue("total_time_sec_avg"), 0.001);
    }

    @Test
    public void testEdgeCases() throws Exception {
        List<Row> rows = new ArrayList<>();
        
        // Test with zero values
        Row row1 = new Row();
        row1.setValue("size", new ByteSize("0B"));
        row1.setValue("time", new TimeDuration("0ms"));
        rows.add(row1);

        // Test with large values
        Row row2 = new Row();
        row2.setValue("size", new ByteSize("1TB"));
        row2.setValue("time", new TimeDuration("24h"));
        rows.add(row2);

        String[] directive = new String[] {
            "aggregate-stats :size :time total_size_mb total_time_sec"
        };

        List<Row> results = TestingRig.execute(directive, rows);

        Assert.assertEquals(1, results.size());
        Row result = results.get(0);

        // Verify handling of large numbers
        Assert.assertTrue(result.getValue("total_size_mb") instanceof Double);
        Assert.assertTrue(result.getValue("total_time_sec") instanceof Double);
    }

    @Test(expected = DirectiveExecutionException.class)
    public void testInvalidInput() throws Exception {
        List<Row> rows = new ArrayList<>();
        
        // Create row with invalid data
        Row row = new Row();
        row.setValue("size", "invalid");
        row.setValue("time", "invalid");
        rows.add(row);

        String[] directive = new String[] {
            "aggregate-stats :size :time total_size_mb total_time_sec"
        };

        // Should throw DirectiveExecutionException
        TestingRig.execute(directive, rows);
    }

    @Test
    public void testEmptyInput() throws Exception {
        List<Row> rows = new ArrayList<>();

        String[] directive = new String[] {
            "aggregate-stats :size :time total_size_mb total_time_sec"
        };

        List<Row> results = TestingRig.execute(directive, rows);

        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        Assert.assertEquals(0.0, result.getValue("total_size_mb"));
        Assert.assertEquals(0.0, result.getValue("total_time_sec"));
    }
}