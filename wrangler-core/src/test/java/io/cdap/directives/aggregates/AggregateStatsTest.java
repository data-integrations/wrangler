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

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.cdap.etl.api.StageMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the AggregateStats directive.
 */
public class AggregateStatsTest {
    private final AggregateStats directive = new AggregateStats();

    @Mock
    private Arguments args;

    @Mock
    private ExecutorContext context;

    @Mock
    private StageMetrics metrics;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(context.getMetrics()).thenReturn(metrics);
    }

    @Test
    public void testBasicAggregation() throws DirectiveParseException {
        // Create test data
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.add("data_transfer_size", "1MB");
        row1.add("response_time", "100ms");
        rows.add(row1);

        Row row2 = new Row();
        row2.add("data_transfer_size", "2MB");
        row2.add("response_time", "200ms");
        rows.add(row2);

        // Mock arguments
        ColumnName sourceSizeCol = new ColumnName("data_transfer_size");
        ColumnName sourceTimeCol = new ColumnName("response_time");
        ColumnName targetSizeCol = new ColumnName("total_size_mb");
        ColumnName targetTimeCol = new ColumnName("total_time_sec");

        when(args.value("source-size-column")).thenReturn(sourceSizeCol);
        when(args.value("source-time-column")).thenReturn(sourceTimeCol);
        when(args.value("target-size-column")).thenReturn(targetSizeCol);
        when(args.value("target-time-column")).thenReturn(targetTimeCol);

        directive.initialize(args);

        // Execute directive
        List<Row> results = directive.execute(rows, context);

        // Verify results
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        Assert.assertEquals("3MB", result.getValue("total_size_mb"));
        Assert.assertEquals("300ms", result.getValue("total_time_sec"));
        Assert.assertEquals(2, result.getValue("count"));
    }

    @Test
    public void testWithInvalidValues() throws DirectiveParseException {
        // Create test data with some invalid values
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.add("data_transfer_size", "1MB");
        row1.add("response_time", "100ms");
        rows.add(row1);

        Row row2 = new Row();
        row2.add("data_transfer_size", "invalid");
        row2.add("response_time", "200ms");
        rows.add(row2);

        Row row3 = new Row();
        row3.add("data_transfer_size", "2MB");
        row3.add("response_time", "invalid");
        rows.add(row3);

        // Mock arguments
        ColumnName sourceSizeCol = new ColumnName("data_transfer_size");
        ColumnName sourceTimeCol = new ColumnName("response_time");
        ColumnName targetSizeCol = new ColumnName("total_size_mb");
        ColumnName targetTimeCol = new ColumnName("total_time_sec");

        when(args.value("source-size-column")).thenReturn(sourceSizeCol);
        when(args.value("source-time-column")).thenReturn(sourceTimeCol);
        when(args.value("target-size-column")).thenReturn(targetSizeCol);
        when(args.value("target-time-column")).thenReturn(targetTimeCol);

        directive.initialize(args);

        // Execute directive
        List<Row> results = directive.execute(rows, context);

        // Verify results - should only count valid values
        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        Assert.assertEquals("3MB", result.getValue("total_size_mb"));
        Assert.assertEquals("300ms", result.getValue("total_time_sec"));
        Assert.assertEquals(3, result.getValue("count"));
    }

    @Test(expected = DirectiveParseException.class)
    public void testWithMissingColumns() throws DirectiveParseException {
        // Mock arguments with missing column
        ColumnName sizeColumn = new ColumnName("data_transfer_size");
        when(args.value("source-size-column")).thenReturn(sizeColumn);
        // Missing source-time-column
        when(args.value("source-time-column")).thenReturn(null);

        directive.initialize(args);
    }
}