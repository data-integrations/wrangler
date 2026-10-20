package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.junit.Assert;
import org.junit.Test;

public class ParticularComponentTest {

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
}