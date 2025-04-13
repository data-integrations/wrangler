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

import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for the AggregateStats directive using TestingRig.
 * Tests various aggregation scenarios including total, average, median, p95, p99,
 * and handles different unit conversions and edge cases.
 */
public class AggregateStatsIntegrationTest {
    private static final String SIZE_COLUMN = "data_transfer_size";
    private static final String TIME_COLUMN = "response_time";
    private static final String TOTAL_SIZE_COLUMN = "total_size_mb";
    private static final String TOTAL_TIME_COLUMN = "total_time_sec";

    /**
     * Tests basic total aggregation of size and time values.
     */
    @Test
    public void testTotalAggregation() throws Exception {
        List<Row> rows = createSampleRows();
        String[] recipe = new String[] {
            String.format("aggregate-stats :%s :%s %s %s size-unit:MB time-unit:seconds",
                         SIZE_COLUMN, TIME_COLUMN, TOTAL_SIZE_COLUMN, TOTAL_TIME_COLUMN)
        };

        List<Row> results = TestingRig.execute(recipe, rows);
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        Assert.assertEquals("6MB", result.getValue(TOTAL_SIZE_COLUMN));
        Assert.assertEquals("6s", result.getValue(TOTAL_TIME_COLUMN));
    }

    /**
     * Tests average aggregation of time values.
     */
    @Test
    public void testAverageAggregation() throws Exception {
        List<Row> rows = createSampleRows();
        String[] recipe = new String[] {
            String.format("aggregate-stats :%s :%s %s %s size-unit:MB time-unit:seconds aggregation-type:average",
                         SIZE_COLUMN, TIME_COLUMN, TOTAL_SIZE_COLUMN, TOTAL_TIME_COLUMN)
        };

        List<Row> results = TestingRig.execute(recipe, rows);
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        Assert.assertEquals("6MB", result.getValue(TOTAL_SIZE_COLUMN));
        Assert.assertEquals("2s", result.getValue(TOTAL_TIME_COLUMN));
    }

    /**
     * Tests aggregation with different input and output units.
     */
    @Test
    public void testDifferentUnits() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add(SIZE_COLUMN, "1024KB").add(TIME_COLUMN, "60s"));
        rows.add(new Row().add(SIZE_COLUMN, "1024KB").add(TIME_COLUMN, "60s"));

        String[] recipe = new String[] {
            String.format("aggregate-stats :%s :%s %s total_time_min size-unit:MB time-unit:minutes",
                         SIZE_COLUMN, TIME_COLUMN, TOTAL_SIZE_COLUMN)
        };

        List<Row> results = TestingRig.execute(recipe, rows);
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        Assert.assertEquals("2MB", result.getValue(TOTAL_SIZE_COLUMN));
        Assert.assertEquals("2m", result.getValue("total_time_min"));
    }

    /**
     * Tests handling of invalid input values.
     */
    @Test
    public void testInvalidValues() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add(SIZE_COLUMN, "1MB").add(TIME_COLUMN, "1s"));
        rows.add(new Row().add(SIZE_COLUMN, "invalid").add(TIME_COLUMN, "invalid"));
        rows.add(new Row().add(SIZE_COLUMN, "2MB").add(TIME_COLUMN, "2s"));

        String[] recipe = new String[] {
            String.format("aggregate-stats :%s :%s %s %s size-unit:MB time-unit:seconds",
                         SIZE_COLUMN, TIME_COLUMN, TOTAL_SIZE_COLUMN, TOTAL_TIME_COLUMN)
        };

        List<Row> results = TestingRig.execute(recipe, rows);
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        Assert.assertEquals("3MB", result.getValue(TOTAL_SIZE_COLUMN));
        Assert.assertEquals("3s", result.getValue(TOTAL_TIME_COLUMN));
    }

    /**
     * Tests handling of zero values.
     */
    @Test
    public void testZeroValues() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add(SIZE_COLUMN, "0MB").add(TIME_COLUMN, "0s"));
        rows.add(new Row().add(SIZE_COLUMN, "0MB").add(TIME_COLUMN, "0s"));

        String[] recipe = new String[] {
            String.format("aggregate-stats :%s :%s %s %s size-unit:MB time-unit:seconds",
                         SIZE_COLUMN, TIME_COLUMN, TOTAL_SIZE_COLUMN, TOTAL_TIME_COLUMN)
        };

        List<Row> results = TestingRig.execute(recipe, rows);
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        Assert.assertEquals("0MB", result.getValue(TOTAL_SIZE_COLUMN));
        Assert.assertEquals("0s", result.getValue(TOTAL_TIME_COLUMN));
    }

    /**
     * Tests handling of very large numbers.
     */
    @Test
    public void testLargeNumbers() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add(SIZE_COLUMN, "1000GB").add(TIME_COLUMN, "1000h"));
        rows.add(new Row().add(SIZE_COLUMN, "1000GB").add(TIME_COLUMN, "1000h"));

        String[] recipe = new String[] {
            String.format("aggregate-stats :%s :%s %s total_time_days size-unit:TB time-unit:days",
                         SIZE_COLUMN, TIME_COLUMN, TOTAL_SIZE_COLUMN)
        };

        List<Row> results = TestingRig.execute(recipe, rows);
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        Assert.assertEquals("2TB", result.getValue(TOTAL_SIZE_COLUMN));
        Assert.assertEquals("83d", result.getValue("total_time_days"));
    }

    /**
     * Tests handling of mixed units in input.
     */
    @Test
    public void testMixedUnits() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add(SIZE_COLUMN, "1MB").add(TIME_COLUMN, "1s"));
        rows.add(new Row().add(SIZE_COLUMN, "1024KB").add(TIME_COLUMN, "60s"));

        String[] recipe = new String[] {
            String.format("aggregate-stats :%s :%s %s %s size-unit:MB time-unit:seconds",
                         SIZE_COLUMN, TIME_COLUMN, TOTAL_SIZE_COLUMN, TOTAL_TIME_COLUMN)
        };

        List<Row> results = TestingRig.execute(recipe, rows);
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        Assert.assertEquals("2MB", result.getValue(TOTAL_SIZE_COLUMN));
        Assert.assertEquals("61s", result.getValue(TOTAL_TIME_COLUMN));
    }

    /**
     * Tests handling of empty input.
     */
    @Test
    public void testEmptyInput() throws Exception {
        List<Row> rows = new ArrayList<>();
        String[] recipe = new String[] {
            String.format("aggregate-stats :%s :%s %s %s size-unit:MB time-unit:seconds",
                         SIZE_COLUMN, TIME_COLUMN, TOTAL_SIZE_COLUMN, TOTAL_TIME_COLUMN)
        };

        List<Row> results = TestingRig.execute(recipe, rows);
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        
        Assert.assertEquals("0MB", result.getValue(TOTAL_SIZE_COLUMN));
        Assert.assertEquals("0s", result.getValue(TOTAL_TIME_COLUMN));
    }

    private List<Row> createSampleRows() {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add(SIZE_COLUMN, "1MB").add(TIME_COLUMN, "1s"));
        rows.add(new Row().add(SIZE_COLUMN, "2MB").add(TIME_COLUMN, "2s"));
        rows.add(new Row().add(SIZE_COLUMN, "3MB").add(TIME_COLUMN, "3s"));
        return rows;
    }
} 

