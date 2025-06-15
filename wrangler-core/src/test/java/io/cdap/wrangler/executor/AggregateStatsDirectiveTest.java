package io.cdap.wrangler.executor;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.parser.MapArguments;
import io.cdap.wrangler.TestingRig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AggregateStatsDirectiveTest {

    @Mock
    private ExecutorContext context;

    @Mock
    private TransientStore store;

    private AggregateStatsDirective directive;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        directive = new AggregateStatsDirective();
        when(context.getTransientStore()).thenReturn(store);
    }

    @Test
    public void testDefine() {
        assertEquals("aggregate-stats", directive.define().getDirectiveName());
    }

    @Test
    public void testExecute() throws Exception {
        // Create usage definition
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
        builder.define("size_column", TokenType.COLUMN_NAME);
        builder.define("time_column", TokenType.COLUMN_NAME);
        UsageDefinition definition = builder.build();

        // Create token group
        io.cdap.wrangler.api.TokenGroup group = new io.cdap.wrangler.api.TokenGroup();
        group.add(new ColumnName("size"));
        group.add(new ColumnName("time"));

        // Initialize directive with arguments
        Arguments args = new MapArguments(definition, group);
        directive.initialize(args);

        // Create test rows
        List<Row> rows = new ArrayList<>();
        Row row1 = new Row();
        row1.add("size", new ByteSize("1MB"));
        row1.add("time", new TimeDuration("1s"));
        rows.add(row1);

        Row row2 = new Row();
        row2.add("size", new ByteSize("2MB"));
        row2.add("time", new TimeDuration("2s"));
        rows.add(row2);

        // Execute directive
        List<Row> result = directive.execute(rows, context);

        // Verify store interactions
        verify(store).increment(eq(TransientVariableScope.GLOBAL), eq("total_bytes"), eq(3L * 1024 * 1024));
        verify(store).increment(eq(TransientVariableScope.GLOBAL), eq("total_nanos"), eq(3L * 1000 * 1000 * 1000));

        // Verify rows are returned unchanged
        assertEquals(rows, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExecuteWithInvalidSize() throws Exception {
        // Create usage definition
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
        builder.define("size_column", TokenType.COLUMN_NAME);
        builder.define("time_column", TokenType.COLUMN_NAME);
        UsageDefinition definition = builder.build();

        // Create token group
        io.cdap.wrangler.api.TokenGroup group = new io.cdap.wrangler.api.TokenGroup();
        group.add(new ColumnName("size"));
        group.add(new ColumnName("time"));

        // Initialize directive with arguments
        Arguments args = new MapArguments(definition, group);
        directive.initialize(args);

        List<Row> rows = new ArrayList<>();
        Row row = new Row();
        row.add("size", "invalid");
        row.add("time", new TimeDuration("1s"));
        rows.add(row);

        directive.execute(rows, context);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExecuteWithInvalidTime() throws Exception {
        // Create usage definition
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
        builder.define("size_column", TokenType.COLUMN_NAME);
        builder.define("time_column", TokenType.COLUMN_NAME);
        UsageDefinition definition = builder.build();

        // Create token group
        io.cdap.wrangler.api.TokenGroup group = new io.cdap.wrangler.api.TokenGroup();
        group.add(new ColumnName("size"));
        group.add(new ColumnName("time"));

        // Initialize directive with arguments
        Arguments args = new MapArguments(definition, group);
        directive.initialize(args);

        List<Row> rows = new ArrayList<>();
        Row row = new Row();
        row.add("size", new ByteSize("1MB"));
        row.add("time", "invalid");
        rows.add(row);

        directive.execute(rows, context);
    }

    @Test
    public void testDestroy() {
        // destroy() should not throw any exceptions
        directive.destroy();
    }

    @Test
    public void testBasicAggregation() throws Exception {
        // Create sample data
        List<Row> rows = new ArrayList<>();
        
        // Add rows with different size and time values
        Row row1 = new Row();
        row1.add("data_transfer_size", new ByteSize("1MB"));
        row1.add("response_time", new TimeDuration("1s"));
        rows.add(row1);

        Row row2 = new Row();
        row2.add("data_transfer_size", new ByteSize("2MB"));
        row2.add("response_time", new TimeDuration("2s"));
        rows.add(row2);

        // Define recipe
        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        // Execute recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Verify results
        assertEquals(1, results.size());
        Row result = results.get(0);
        
        // Expected total size: 3MB (1MB + 2MB)
        assertEquals(3.0, (Double) result.getValue("total_size_mb"), 0.001);
        
        // Expected total time: 3s (1s + 2s)
        assertEquals(3.0, (Double) result.getValue("total_time_sec"), 0.001);
    }

    @Test
    public void testDifferentUnits() throws Exception {
        // Create sample data with different units
        List<Row> rows = new ArrayList<>();
        
        Row row1 = new Row();
        row1.add("data_transfer_size", new ByteSize("1024KB")); // 1MB
        row1.add("response_time", new TimeDuration("1000ms")); // 1s
        rows.add(row1);

        Row row2 = new Row();
        row2.add("data_transfer_size", new ByteSize("2MB"));
        row2.add("response_time", new TimeDuration("2s"));
        rows.add(row2);

        // Define recipe
        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        // Execute recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Verify results
        assertEquals(1, results.size());
        Row result = results.get(0);
        
        // Expected total size: 3MB (1MB + 2MB)
        assertEquals(3.0, (Double) result.getValue("total_size_mb"), 0.001);
        
        // Expected total time: 3s (1s + 2s)
        assertEquals(3.0, (Double) result.getValue("total_time_sec"), 0.001);
    }

    @Test
    public void testDecimalValues() throws Exception {
        // Create sample data with decimal values
        List<Row> rows = new ArrayList<>();
        
        Row row1 = new Row();
        row1.add("data_transfer_size", new ByteSize("1.5MB"));
        row1.add("response_time", new TimeDuration("1.5s"));
        rows.add(row1);

        Row row2 = new Row();
        row2.add("data_transfer_size", new ByteSize("2.5MB"));
        row2.add("response_time", new TimeDuration("2.5s"));
        rows.add(row2);

        // Define recipe
        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        // Execute recipe
        List<Row> results = TestingRig.execute(recipe, rows);

        // Verify results
        assertEquals(1, results.size());
        Row result = results.get(0);
        
        // Expected total size: 4MB (1.5MB + 2.5MB)
        assertEquals(4.0, (Double) result.getValue("total_size_mb"), 0.001);
        
        // Expected total time: 4s (1.5s + 2.5s)
        assertEquals(4.0, (Double) result.getValue("total_time_sec"), 0.001);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSizeFormat() throws Exception {
        List<Row> rows = new ArrayList<>();
        
        Row row = new Row();
        row.add("data_transfer_size", "invalid"); // Invalid size format
        row.add("response_time", new TimeDuration("1s"));
        rows.add(row);

        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        TestingRig.execute(recipe, rows);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeFormat() throws Exception {
        List<Row> rows = new ArrayList<>();
        
        Row row = new Row();
        row.add("data_transfer_size", new ByteSize("1MB"));
        row.add("response_time", "invalid"); // Invalid time format
        rows.add(row);

        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        TestingRig.execute(recipe, rows);
    }
} 