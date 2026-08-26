package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class AggregateStatsTest {

    @Mock
    private Arguments arguments;

    @Mock
    private ExecutorContext context;

    private AggregateStats directive;
    private List<Row> rows;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        directive = new AggregateStats();
        rows = new ArrayList<>();
    }

    @Test
    public void testDefine() {
        assertEquals("aggregate-stats", directive.define().getDirectiveName());
    }

    @Test
    public void testInitialize() {
        // Mock the column name values
        ColumnName byteSizeColumn = new ColumnName("size");
        ColumnName timeDurationColumn = new ColumnName("time");

        when(arguments.value("byte-size-column")).thenReturn(byteSizeColumn);
        when(arguments.value("time-duration-column")).thenReturn(timeDurationColumn);

        directive.initialize(arguments);

        assertEquals("size", directive.getByteSizeColumn());
        assertEquals("time", directive.getTimeDurationColumn());
        assertEquals(0, directive.getTotalBytes());
        assertEquals(0, directive.getTotalNanoseconds());
        assertEquals(0, directive.getRowCount());
    }

    @Test
    public void testExecuteWithByteSize() {
        // Setup
        initializeDirective();

        // Create test rows with ByteSize objects
        Row row1 = new Row();
        row1.add("size", new ByteSize("1MB"));
        row1.add("time", new TimeDuration("1s"));

        Row row2 = new Row();
        row2.add("size", new ByteSize("2MB"));
        row2.add("time", new TimeDuration("2s"));

        rows.add(row1);
        rows.add(row2);

        List<Row> result = directive.execute(rows, context);

        assertEquals(0, result.size()); 
        assertEquals(2, directive.getRowCount());
        assertEquals(3 * 1024 * 1024, directive.getTotalBytes()); // 3MB in bytes
        assertEquals(3_000_000_000L, directive.getTotalNanoseconds()); // 3s in nanoseconds
    }

    @Test
    public void testExecuteWithNumericValues() {
        // Setup
        initializeDirective();

        // Create test rows with numeric values
        Row row1 = new Row();
        row1.add("size", 1024L);
        row1.add("time", 1_000_000_000L);

        Row row2 = new Row();
        row2.add("size", 2048L);
        row2.add("time", 2_000_000_000L);

        rows.add(row1);
        rows.add(row2);

        // Execute
        List<Row> result = directive.execute(rows, context);

        // Verify
        assertEquals(0, result.size());
        assertEquals(2, directive.getRowCount());
        assertEquals(3072, directive.getTotalBytes());
        assertEquals(3_000_000_000L, directive.getTotalNanoseconds());
    }

    @Test
    public void testExecuteWithStringValues() {
        // Setup
        initializeDirective();

        // Create test rows with string values
        Row row1 = new Row();
        row1.add("size", "1KB");
        row1.add("time", "1s");

        Row row2 = new Row();
        row2.add("size", "2KB");
        row2.add("time", "2s");

        rows.add(row1);
        rows.add(row2);

        // Execute
        List<Row> result = directive.execute(rows, context);

        // Verify
        assertEquals(0, result.size());
        assertEquals(2, directive.getRowCount());
        assertEquals(3 * 1024, directive.getTotalBytes()); // 3KB in bytes
        assertEquals(3_000_000_000L, directive.getTotalNanoseconds()); // 3s in nanoseconds
    }

    @Test
    public void testExecuteWithNullValues() {
        // Setup
        initializeDirective();

        // Create test rows with null values
        Row row1 = new Row();
        row1.add("size", null);
        row1.add("time", null);

        Row row2 = new Row();
        row2.add("other_col", "value");

        rows.add(row1);
        rows.add(row2);

        // Execute
        List<Row> result = directive.execute(rows, context);

        // Verify
        assertEquals(0, result.size());
        assertEquals(2, directive.getRowCount());
        assertEquals(0, directive.getTotalBytes());
        assertEquals(0, directive.getTotalNanoseconds());
    }

    @Test
    public void testExecuteWithMixedValues() {
        // Setup
        initializeDirective();

        // Create test rows with mixed value types
        Row row1 = new Row();
        row1.add("size", new ByteSize("1MB"));
        row1.add("time", 1_000_000_000L);

        Row row2 = new Row();
        row2.add("size", "2MB");
        row2.add("time", "2s");

        Row row3 = new Row();
        row3.add("size", 3 * 1024 * 1024L);
        row3.add("time", new TimeDuration("3s"));

        rows.add(row1);
        rows.add(row2);
        rows.add(row3);

        // Execute
        List<Row> result = directive.execute(rows, context);

        // Verify
        assertEquals(0, result.size());
        assertEquals(3, directive.getRowCount());
        assertEquals(6 * 1024 * 1024, directive.getTotalBytes()); // 6MB in bytes
        assertEquals(6_000_000_000L, directive.getTotalNanoseconds()); // 6s in nanoseconds
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExecuteWithInvalidByteSize() {
        // Setup
        initializeDirective();

        // Create test row with invalid byte size
        Row row = new Row();
        row.add("size", "invalid");
        row.add("time", "1s");

        rows.add(row);

        // Execute - should throw IllegalArgumentException
        directive.execute(rows, context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExecuteWithInvalidTimeDuration() {
        // Setup
        initializeDirective();

        // Create test row with invalid time duration
        Row row = new Row();
        row.add("size", "1MB");
        row.add("time", "invalid");

        rows.add(row);

        // Execute - should throw IllegalArgumentException
        directive.execute(rows, context);
    }

    @Test
    public void testExecuteWithTimeDurationNumericString() {
        // Setup
        initializeDirective();

        // Create test row with numeric string for time duration
        Row row = new Row();
        row.add("size", "1MB");
        row.add("time", "1000000");

        rows.add(row);

        // Execute
        List<Row> result = directive.execute(rows, context);

        // Verify
        assertEquals(0, result.size());
        assertEquals(1, directive.getRowCount());
        assertEquals(1024 * 1024, directive.getTotalBytes());
        assertEquals(1000000000000L, directive.getTotalNanoseconds());
    }

    @Test
    public void testDestroy() {
        // Just to cover the method
        directive.destroy();
    }

    private void initializeDirective() {
        ColumnName byteSizeColumn = new ColumnName("size");
        ColumnName timeDurationColumn = new ColumnName("time");

        when(arguments.value("byte-size-column")).thenReturn(byteSizeColumn);
        when(arguments.value("time-duration-column")).thenReturn(timeDurationColumn);

        directive.initialize(arguments);
    }
}