package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.executor.ExecutionContext;
import io.cdap.wrangler.api.executor.ExecutorContext;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.service.testing.TestingRig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AggregateDirectiveTest {

    private AggregateDirective directive;
    private List<Row> rows;

    @Before
    public void setUp() {
        directive = new AggregateDirective();
        rows = new ArrayList<>();
        Row row1 = new Row();
        row1.add("byteSize", new ByteSize("10KB"));
        row1.add("timeDuration", new TimeDuration("5s"));
        rows.add(row1);

        Row row2 = new Row();
        row2.add("byteSize", new ByteSize("1MB"));
        row2.add("timeDuration", new TimeDuration("2m"));
        rows.add(row2);
    }

    @Test
    public void testAggregateDirectiveExecution() {
        ExecutionContext context = new ExecutorContext();
        context.addArgument(TokenType.COLUMN_NAME, "byteSize");
        context.addArgument(TokenType.COLUMN_NAME, "timeDuration");
        context.addArgument(TokenType.COLUMN_NAME, "totalSize");
        context.addArgument(TokenType.COLUMN_NAME, "totalTime");
        context.addArgument(TokenType.TEXT, "MB");
        context.addArgument(TokenType.TEXT, "s");

        directive.initialize(context);
        List<Row> resultRows = directive.execute(rows, context);
        directive.finalize(context);

        Row resultRow = resultRows.get(0);
        Assert.assertEquals(1.01, resultRow.getValue("totalSize"));
        Assert.assertEquals(125, resultRow.getValue("totalTime"));
    }

    @Test
    public void testAggregateStatsDirective() throws Exception {
        // Create sample input data
        List<Row> inputRows = new ArrayList<>();
        Row row1 = new Row();
        row1.add("data_transfer_size", new ByteSize("10KB"));
        row1.add("response_time", new TimeDuration("5s"));
        inputRows.add(row1);

        Row row2 = new Row();
        row2.add("data_transfer_size", new ByteSize("1MB"));
        row2.add("response_time", new TimeDuration("2m"));
        inputRows.add(row2);

        // Define the recipe
        String[] recipe = new String[] {
            "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        // Execute the recipe using TestingRig
        List<Row> outputRows = TestingRig.execute(recipe, inputRows);

        // Calculate expected values
        double expectedTotalSizeInMB = (10 * 1024 + 1 * 1024 * 1024) / (1024.0 * 1024.0);
        double expectedTotalTimeInSeconds = (5 + 2 * 60);

        // Verify the output
        Assert.assertEquals(1, outputRows.size());
        Row outputRow = outputRows.get(0);
        Assert.assertEquals(expectedTotalSizeInMB, (double) outputRow.getValue("total_size_mb"), 0.001);
        Assert.assertEquals(expectedTotalTimeInSeconds, (double) outputRow.getValue("total_time_sec"), 0.001);
    }
}
