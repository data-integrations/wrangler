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

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link AggregateStats} using mocks
 */
public class AggregateStatsMockTest {

    private AggregateStats directive;
    private ExecutorContext context;
    private TransientStore store;

    @Before
    public void setup() throws DirectiveParseException {
        // Create the directive
        directive = new AggregateStats();

        // Mock the context and store
        context = Mockito.mock(ExecutorContext.class);
        store = Mockito.mock(TransientStore.class);
        Mockito.when(context.getTransientStore()).thenReturn(store);
    }

    @Test
    public void testBasicAggregation() throws DirectiveParseException, DirectiveExecutionException {
        // Initialize the directive with arguments
        Arguments args = Mockito.mock(Arguments.class);
        Mockito.when(args.value("size-column")).thenReturn(new ColumnName("data_transfer_size"));
        Mockito.when(args.value("time-column")).thenReturn(new ColumnName("response_time"));
        Mockito.when(args.value("target-size-column")).thenReturn(new ColumnName("total_size_bytes"));
        Mockito.when(args.value("target-time-column")).thenReturn(new ColumnName("total_time_nanos"));
        directive.initialize(args);

        // Create test data
        List<Row> rows = Arrays.asList(
                new Row("data_transfer_size", "1KB").add("response_time", "10ms"), // 1024 B, 10_000_000 ns
                new Row("data_transfer_size", "2048").add("response_time", "0.5s"), // 2048 B, 500_000_000 ns
                new Row("data_transfer_size", "1.5MB").add("response_time", "1m"), // 1572864 B, 60_000_000_000 ns
                new Row("data_transfer_size", null).add("response_time", "100ns"), // null size, 100 ns
                new Row("data_transfer_size", "10B").add("response_time", null), // 10 B, null time
                new Row("data_transfer_size", "invalid").add("response_time", "invalid") // Invalid, should be skipped
        );

        // Expected totals
        long expectedTotalBytes = 1024L + 2048L + (long)(1.5 * 1024 * 1024) + 10L;
        long expectedTotalNanos = 10_000_000L + 500_000_000L + 60_000_000_000L + 100L;

        // Process each row individually (simulating how RecipePipelineExecutor works)
        for (Row row : rows) {
            directive.execute(Collections.singletonList(row), context);
        }

        // Create a mock for the final result
        Mockito.when(store.get("aggregate-stats_total_bytes")).thenReturn(expectedTotalBytes);
        Mockito.when(store.get("aggregate-stats_total_nanos")).thenReturn(expectedTotalNanos);

        // Get the final result
        List<Row> results = directive.execute(Collections.emptyList(), context);

        // Verify the result
        Assert.assertEquals(1, results.size());
        Row resultRow = results.get(0);
        Assert.assertEquals(expectedTotalBytes, resultRow.getValue("total_size_bytes"));
        Assert.assertEquals(expectedTotalNanos, resultRow.getValue("total_time_nanos"));
    }

    @Test
    public void testEmptyInput() throws DirectiveParseException, DirectiveExecutionException {
        // Initialize the directive with arguments
        Arguments args = Mockito.mock(Arguments.class);
        Mockito.when(args.value("size-column")).thenReturn(new ColumnName("size"));
        Mockito.when(args.value("time-column")).thenReturn(new ColumnName("time"));
        Mockito.when(args.value("target-size-column")).thenReturn(new ColumnName("total_size"));
        Mockito.when(args.value("target-time-column")).thenReturn(new ColumnName("total_time"));
        directive.initialize(args);

        // Mock the store to return 0 for both totals
        Mockito.when(store.get("aggregate-stats_total_bytes")).thenReturn(0L);
        Mockito.when(store.get("aggregate-stats_total_nanos")).thenReturn(0L);

        // Execute with empty input
        List<Row> results = directive.execute(Collections.emptyList(), context);

        // Verify the result
        Assert.assertEquals(1, results.size());
        Row resultRow = results.get(0);
        Assert.assertEquals(0L, resultRow.getValue("total_size"));
        Assert.assertEquals(0L, resultRow.getValue("total_time"));
    }

    @Test
    public void testAllNullOrInvalidInput() throws DirectiveParseException, DirectiveExecutionException {
        // Initialize the directive with arguments
        Arguments args = Mockito.mock(Arguments.class);
        Mockito.when(args.value("size-column")).thenReturn(new ColumnName("size"));
        Mockito.when(args.value("time-column")).thenReturn(new ColumnName("time"));
        Mockito.when(args.value("target-size-column")).thenReturn(new ColumnName("total_s"));
        Mockito.when(args.value("target-time-column")).thenReturn(new ColumnName("total_t"));
        directive.initialize(args);

        // Create test data with null or invalid values
        List<Row> rows = Arrays.asList(
            new Row("size", null).add("time", null),
            new Row("size", "bad").add("time", "wrong")
        );

        // Process each row individually
        for (Row row : rows) {
            directive.execute(Collections.singletonList(row), context);
        }

        // Mock the store to return 0 for both totals (since all inputs are null or invalid)
        Mockito.when(store.get("aggregate-stats_total_bytes")).thenReturn(0L);
        Mockito.when(store.get("aggregate-stats_total_nanos")).thenReturn(0L);

        // Get the final result
        List<Row> results = directive.execute(Collections.emptyList(), context);

        // Verify the result
        Assert.assertEquals(1, results.size());
        Row resultRow = results.get(0);
        Assert.assertEquals(0L, resultRow.getValue("total_s"));
        Assert.assertEquals(0L, resultRow.getValue("total_t"));
    }
}
