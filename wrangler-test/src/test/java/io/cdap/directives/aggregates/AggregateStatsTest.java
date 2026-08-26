/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package io.cdap.directives.aggregates;

import java.util.Arrays;
import java.util.List;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.test.TestingRig;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for AggregateStats directive.
 */
public class AggregateStatsTest {

    @Test
    public void testAggregateStatsDirective() throws Exception {
        List<Row> rows = Arrays.asList(
                new Row("size", 1048576L).add("duration", 1000L),  // 1 MB, 1 sec
                new Row("size", 2097152L).add("duration", 2000L),  // 2 MB, 2 sec
                new Row("size", 3145728L).add("duration", 3000L)   // 3 MB, 3 sec
        );

        String[] recipe = {
                "aggregate-stats :size :duration total_size_mb total_time_sec"
        };

        List<Row> results = TestingRig.execute(recipe, rows);

        Assert.assertEquals(1, results.size());
        Row result = results.get(0);

        double expectedSizeMB = 6.0;  // Total size in MB
        double expectedTimeSec = 6.0; // Total duration in seconds

        Assert.assertEquals(expectedSizeMB, ((Number) result.getValue("total_size_mb")).doubleValue(), 0.01);
        Assert.assertEquals(expectedTimeSec, ((Number) result.getValue("total_time_sec")).doubleValue(), 0.01);
    }
}