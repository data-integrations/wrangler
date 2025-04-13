package io.cdap.directives.aggregates;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AggregateDirectiveTest {

    @Test
    public void testDefineMethod() {
        AggregateDirective directive = new AggregateDirective();
        UsageDefinition usageDefinition = directive.define();

        // Verify the name of the directive
        Assert.assertEquals("aggregate-stats", usageDefinition.getDirectiveName());

        // Verify the number of arguments defined
        Assert.assertEquals(7, usageDefinition.getTokens().size());

        // Verify each argument definition
        Assert.assertTrue(usageDefinition.getTokens().stream()
                .anyMatch(def -> def.name().equals("sourceSizeColumn") &&
                        def.type() == TokenType.COLUMN_NAME &&
                        !def.optional()));

        Assert.assertTrue(usageDefinition.getTokens().stream()
                .anyMatch(def -> def.name().equals("sourceTimeColumn") &&
                        def.type() == TokenType.COLUMN_NAME &&
                        !def.optional()));

        Assert.assertTrue(usageDefinition.getTokens().stream()
                .anyMatch(def -> def.name().equals("targetSizeColumn") &&
                        def.type() == TokenType.IDENTIFIER &&
                        !def.optional()));

        Assert.assertTrue(usageDefinition.getTokens().stream()
                .anyMatch(def -> def.name().equals("targetTimeColumn") &&
                        def.type() == TokenType.IDENTIFIER &&
                        !def.optional()));

        Assert.assertTrue(usageDefinition.getTokens().stream()
                .anyMatch(def -> def.name().equals("outputSizeUnit") &&
                        def.type() == TokenType.TEXT &&
                        def.optional()));

        Assert.assertTrue(usageDefinition.getTokens().stream()
                .anyMatch(def -> def.name().equals("outputTimeUnit") &&
                        def.type() == TokenType.TEXT &&
                        def.optional()));

        Assert.assertTrue(usageDefinition.getTokens().stream()
                .anyMatch(def -> def.name().equals("aggregationType") &&
                        def.type() == TokenType.TEXT &&
                        def.optional()));
    }

    @Test
    public void testInitialize() throws Exception {
        // Create a mock Arguments object
        Arguments arguments = new ArgumentsBuilder()
                .add("sourceSizeColumn", ":data_transfer_size")
                .add("sourceTimeColumn", ":response_time")
                .add("targetSizeColumn", "total_size_mb")
                .add("targetTimeColumn", "total_time_sec")
                .add("outputSizeUnit", "MB")
                .add("outputTimeUnit", "s")
                .build();

        // Initialize the directive
        AggregateDirective directive = new AggregateDirective();
        directive.initialize(arguments);

        // Verify the fields are correctly initialized
        Assert.assertEquals(":data_transfer_size", directive.sourceSizeColumn);
        Assert.assertEquals(":response_time", directive.sourceTimeColumn);
        Assert.assertEquals("total_size_mb", directive.targetSizeColumn);
        Assert.assertEquals("total_time_sec", directive.targetTimeColumn);
        Assert.assertEquals("mb", directive.outputSizeUnit); // Lowercased
        Assert.assertEquals("s", directive.outputTimeUnit);
    }

    @Test
    public void testExecute() throws Exception {
        // Prepare input rows
        List<Row> input = Arrays.asList(
                new Row("data_transfer_size", "10MB").add("response_time", "2s"),
                new Row("data_transfer_size", "5MB").add("response_time", "1s")
        );

        // Initialize the directive
        AggregateDirective directive = new AggregateDirective();
        Arguments arguments = new ArgumentsBuilder()
                .add("sourceSizeColumn", "data_transfer_size")
                .add("sourceTimeColumn", "response_time")
                .add("targetSizeColumn", "total_size_mb")
                .add("targetTimeColumn", "total_time_sec")
                .add("outputSizeUnit", "MB")
                .add("outputTimeUnit", "seconds")
                .build();
        directive.initialize(arguments);

        // Execute the directive
        List<Row> output = directive.execute(input, null);

        // Verify the output
        Assert.assertEquals(1, output.size());
        Row result = output.get(0);

        Assert.assertEquals(15.0, result.getValue("total_size_mb")); // 10MB + 5MB
        Assert.assertEquals(3.0, result.getValue("total_time_sec")); // 2s + 1s
    }

    @Test
    public void testAggregateStatsTotal() throws Exception {
        List<Row> input = Arrays.asList(
                new Row("data_transfer_size", "10MB").add("response_time", "2s"),
                new Row("data_transfer_size", "5MB").add("response_time", "1s")
        );

        String[] recipe = {
                "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        List<Row> output = TestingRig.execute(recipe, input);
        System.out.println(output);
        Row result = output.get(0);
        Assert.assertEquals(15.0, result.getValue("total_size_mb")); // 10MB + 5MB
        Assert.assertEquals(3.0, result.getValue("total_time_sec")); // 2s + 1s
    }

    @Test
    public void testNullValues() throws DirectiveExecutionException, DirectiveParseException {
        AggregateDirective directive = new AggregateDirective();

        List<Row> input = Arrays.asList(
                new Row("size", null).add("time", "2s"),
                new Row("size", "1MB").add("time", null)
        );

        directive.initialize(new ArgumentsBuilder()
                .add("sourceSizeColumn", ":size")
                .add("sourceTimeColumn", ":time")
                .add("targetSizeColumn", "total_size_mb")
                .add("targetTimeColumn", "total_time_sec")
                .build());

        List<Row> output = directive.execute(input, null);

        Row result = output.get(0);

        Assert.assertEquals(1.0, result.getValue("total_size_mb")); // Only 1MB is valid
        Assert.assertEquals(2.0, result.getValue("total_time_sec")); // Only 2s is valid
    }

    @Test(expected = DirectiveExecutionException.class)
    public void testInvalidInput() throws DirectiveExecutionException, DirectiveParseException {
        AggregateDirective directive = new AggregateDirective();

        List<Row> input = Collections.singletonList(
                new Row("size", "invalid").add("time", "invalid")
        );

        directive.initialize(new ArgumentsBuilder()
                .add("sourceSizeColumn", ":size")
                .add("sourceTimeColumn", ":time")
                .add("targetSizeColumn", "total_size_mb")
                .add("targetTimeColumn", "total_time_sec")
                .build());

        directive.execute(input, null);
    }
}