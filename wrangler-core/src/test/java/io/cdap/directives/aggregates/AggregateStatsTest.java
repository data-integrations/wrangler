/*
 * Copyright © 2025 CDAP
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
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link AggregateStats}
 */
public class AggregateStatsTest {

    @Test
    public void testBasicAggregation() throws RecipeException, DirectiveExecutionException,
                                            DirectiveParseException, DirectiveLoadException {
        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time :total_size_bytes :total_time_nanos"
        };

        List<Row> rows = Arrays.asList(
                new Row("data_transfer_size", "1KB").add("response_time", "10ms"), // 1024 B, 10_000_000 ns
                new Row("data_transfer_size", "2048").add("response_time", "0.5s"), // 2048 B, 500_000_000 ns
                new Row("data_transfer_size", "1.5MB").add("response_time", "1m"), // 1572864 B, 60_000_000_000 ns
                new Row("data_transfer_size", null).add("response_time", "100ns"), // null size, 100 ns
                new Row("data_transfer_size", "10B").add("response_time", null), // 10 B, null time
                new Row("data_transfer_size", "invalid").add("response_time", "invalid") // Invalid, should be skipped
        );

        // Expected totals:
        // Bytes: 1024 + 2048 + 1572864 + 0 + 10 = 1,575,946 bytes
        // Nanos: 10_000_000 + 500_000_000 + 60_000_000_000 + 100 + 0 = 60,510,000,100 ns
        long expectedTotalBytes = 1024L + 2048L + (long) (1.5 * 1024 * 1024) + 10L;
        long expectedTotalNanos = 10_000_000L + 500_000_000L + 60_000_000_000L + 100L;

        // Execute the directive on all rows at once
        List<Row> results = TestingRig.execute(recipe, rows);

        // Aggregation directives should return exactly one row
        Assert.assertEquals(1, results.size());

        Row resultRow = results.get(0);
        Assert.assertEquals(expectedTotalBytes, resultRow.getValue("total_size_bytes"));
        Assert.assertEquals(expectedTotalNanos, resultRow.getValue("total_time_nanos"));
    }

    @Test
    public void testEmptyInput() throws RecipeException, DirectiveExecutionException,
                                        DirectiveParseException, DirectiveLoadException {
         String[] recipe = new String[] {
                "aggregate-stats :size :time :total_size :total_time"
        };
        List<Row> rows = Arrays.asList(); // Empty input

        // Execute with empty input
        List<Row> results = TestingRig.execute(recipe, rows);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(0L, results.get(0).getValue("total_size"));
        Assert.assertEquals(0L, results.get(0).getValue("total_time"));
    }

    @Test
    public void testAllNullOrInvalidInput() throws RecipeException, DirectiveExecutionException,
                                                DirectiveParseException, DirectiveLoadException {
         String[] recipe = new String[] {
                "aggregate-stats :size :time :total_s :total_t"
        };
        List<Row> rows = Arrays.asList(
            new Row("size", null).add("time", null),
            new Row("size", "bad").add("time", "wrong")
        );

        // Execute with all null or invalid input
        List<Row> results = TestingRig.execute(recipe, rows);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(0L, results.get(0).getValue("total_s"));
        Assert.assertEquals(0L, results.get(0).getValue("total_t"));
    }

     // TODO: Add tests for optional arguments (units, aggregation type) once implemented.

}
// End of file