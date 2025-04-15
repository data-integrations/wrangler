package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;

public class AggregateStatsTest {

    /**
     * Creates and configures an AggregateStats directive with the given parameters
     */
    private AggregateStats createDirective(String column, String type, String mode, String unit, String into)
            throws DirectiveParseException {
        AggregateStats directive = new AggregateStats();

        // Create mock Arguments
        Arguments args = Mockito.mock(Arguments.class);

        // Configure mock Arguments to return expected values and handle null checks
        when(args.value("column")).thenReturn(new ColumnName(column));
        when(args.value("type")).thenReturn(new Text(type));
        when(args.value("mode")).thenReturn(new Text(mode));
        when(args.value("unit")).thenReturn(new Text(unit));
        when(args.value("into")).thenReturn(new Text(into));

        // Make contains return true to avoid NPEs
        when(args.contains("column")).thenReturn(true);
        when(args.contains("type")).thenReturn(true);
        when(args.contains("mode")).thenReturn(true);
        when(args.contains("unit")).thenReturn(true);
        when(args.contains("into")).thenReturn(true);

        // Initialize the directive
        directive.initialize(args);
        return directive;
    }

    @Test
    public void testAggregation() throws Exception {
        // Create an instance with the desired configuration
        AggregateStats directive = createDirective("bytes", "BYTESIZE", "total", "MB", "total_bytes");

        List<Row> rows = Arrays.asList(
                new Row("bytes", "10KB"),
                new Row("bytes", "20KB"),
                new Row("bytes", "30KB"),
                new Row("bytes", "40KB")
        );

        // Mock context that returns true for isEndPartition
        ExecutorContext context = Mockito.mock(ExecutorContext.class);
        when(context.isEndPartition()).thenReturn(true);

        // Execute the directive
        List<Row> results = directive.execute(rows, context);

        // After aggregation, we should still have all original rows
        Assert.assertEquals(4, results.size());

        // Last row should have the aggregated value
        Row lastRow = results.get(results.size() - 1);

        Object totalBytes = lastRow.getValue("total_bytes");
        Assert.assertNotNull("total_bytes column should exist", totalBytes);

        // 10KB + 20KB + 30KB + 40KB = 100KB = 0.10MB
        Assert.assertEquals("0.10MB", totalBytes.toString());
    }

    @Test
    public void testAggregateStatsDirective() throws Exception {
        // Create an instance with the desired configuration
        AggregateStats directive = createDirective("bytes", "BYTESIZE", "avg", "KB", "avg_bytes");

        List<Row> rows = Arrays.asList(
                new Row("bytes", "100B"),
                new Row("bytes", "200B"),
                new Row("bytes", "300B"),
                new Row("bytes", "400B")
        );

        // Mock context that returns true for isEndPartition
        ExecutorContext context = Mockito.mock(ExecutorContext.class);
        when(context.isEndPartition()).thenReturn(true);

        // Execute the directive
        List<Row> results = directive.execute(rows, context);

        // After aggregation, we should still have all original rows
        Assert.assertEquals(4, results.size());

        // Last row should have the aggregated value
        Row lastRow = results.get(results.size() - 1);

        Object avgBytes = lastRow.getValue("avg_bytes");
        Assert.assertNotNull("avg_bytes column should exist", avgBytes);

        // (100 + 200 + 300 + 400) / 4 = 250B = 0.24KB
        Assert.assertEquals("0.24KB", avgBytes.toString());
    }

    @Test
    public void testTimeAggregation() throws Exception {
        // Create an instance with the desired configuration
        AggregateStats directive = createDirective("duration", "TIMEDURATION", "avg", "s", "avg_duration");

        List<Row> rows = Arrays.asList(
                new Row("duration", "1000ms"),
                new Row("duration", "2000ms"),
                new Row("duration", "3000ms"),
                new Row("duration", "4000ms")
        );

        // Mock context that returns true for isEndPartition
        ExecutorContext context = Mockito.mock(ExecutorContext.class);
        when(context.isEndPartition()).thenReturn(true);

        // Execute the directive
        List<Row> results = directive.execute(rows, context);

        // After aggregation, we should still have all original rows
        Assert.assertEquals(4, results.size());

        // Last row should have the aggregated average value
        Row lastRow = results.get(results.size() - 1);

        Object avgDuration = lastRow.getValue("avg_duration");
        Assert.assertNotNull("avg_duration column should exist", avgDuration);

        // (1000 + 2000 + 3000 + 4000) / 4 = 2500ms = 2.50s
        Assert.assertEquals("2.50s", avgDuration.toString());
    }

    @Test
    public void testEmptyInputRows() throws Exception {
        // Create an instance with the desired configuration
        AggregateStats directive = createDirective("bytes", "BYTESIZE", "total", "MB", "total_bytes");

        // Empty list of rows
        List<Row> rows = Collections.emptyList();

        // Mock context that returns true for isEndPartition
        ExecutorContext context = Mockito.mock(ExecutorContext.class);
        when(context.isEndPartition()).thenReturn(true);

        // Execute the directive - should handle empty input gracefully
        List<Row> results = directive.execute(rows, context);

        // Should return empty list
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void testInvalidData() throws Exception {
        // Create an instance with the desired configuration
        AggregateStats directive = createDirective("bytes", "BYTESIZE", "total", "MB", "total_bytes");

        List<Row> rows = Arrays.asList(
                new Row("bytes", "10KB"),
                new Row("bytes", null),  // null value should be skipped
                new Row("other", "30KB"), // missing column should be skipped
                new Row("bytes", "40KB")
        );

        // Mock context that returns true for isEndPartition
        ExecutorContext context = Mockito.mock(ExecutorContext.class);
        when(context.isEndPartition()).thenReturn(true);

        // Execute the directive
        List<Row> results = directive.execute(rows, context);

        // Should have processed the valid rows
        Assert.assertEquals(4, results.size());

        // Last row should have the aggregated value from valid rows only
        Row lastRow = results.get(results.size() - 1);
        Object totalBytes = lastRow.getValue("total_bytes");

        // 10KB + 40KB = 50KB = 0.05MB
        Assert.assertEquals("0.05MB", totalBytes.toString());
    }

    @Test
    public void testEdgeCases() throws Exception {
        // Create an instance that calculates min value
        AggregateStats directive = createDirective("bytes", "BYTESIZE", "min", "KB", "min_bytes");

        List<Row> rows = Arrays.asList(
                new Row("bytes", "100B"),
                new Row("bytes", "200B"),
                new Row("bytes", "50B"),  // This should be the min
                new Row("bytes", "400B")
        );

        // Mock context that returns true for isEndPartition
        ExecutorContext context = Mockito.mock(ExecutorContext.class);
        when(context.isEndPartition()).thenReturn(true);

        // Execute the directive
        List<Row> results = directive.execute(rows, context);

        // Last row should have the minimum value
        Row lastRow = results.get(results.size() - 1);
        Object minBytes = lastRow.getValue("min_bytes");

        // Min value is 50B = 0.05KB
        Assert.assertEquals("0.05KB", minBytes.toString());

        // Create a new instance that calculates max value
        directive = createDirective("bytes", "BYTESIZE", "max", "KB", "max_bytes");

        // Execute with the same rows
        results = directive.execute(rows, context);

        // Last row should have the maximum value
        lastRow = results.get(results.size() - 1);
        Object maxBytes = lastRow.getValue("max_bytes");

        // Max value is 400B = 0.39KB
        Assert.assertEquals("0.39KB", maxBytes.toString());
    }
}