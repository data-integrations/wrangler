/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for TimeDuration token parsing in the wrangler grammar.
 */
public class TimeDurationParserTest {

    /**
     * Helper to create a parser instance for testing
     */
    private RecipeParser createParser(String recipe) {
        CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
                SystemDirectiveRegistry.INSTANCE);
        return new GrammarBasedParser(Contexts.SYSTEM, recipe, registry);
    }

    @Test
    public void testBasicTimeDurationParsing() throws Exception {
        // Parse a simple directive that would take a TIME_DURATION argument
        RecipeParser parser = createParser("set-retention 2h;");
        List<io.cdap.wrangler.api.Directive> directives = parser.parse();

        // This verifies that the directive parsed successfully
        Assert.assertEquals(1, directives.size());

        // Use the GrammarWalker to access the tokens directly for verification
        new GrammarWalker(new RecipeCompiler(), new NoOpDirectiveContext()).walk("set-retention 2h;",
                (command, tokenGroup) -> {
                    // Verify command name
                    Assert.assertEquals("set-retention", command);

                    // Verify that the second token is a TimeDuration
                    Assert.assertEquals(2, tokenGroup.size());
                    Token token = tokenGroup.get(1);
                    Assert.assertEquals(TokenType.TIME_DURATION, token.type());
                    Assert.assertTrue(token instanceof TimeDuration);

                    TimeDuration duration = (TimeDuration) token;
                    Assert.assertEquals("2h", duration.value());
                    Assert.assertEquals(2 * 60 * 60 * 1000, duration.getMilliseconds());
                });
    }

    @Test
    public void testTimeDurationVariations() throws Exception {
        // Test seconds
        verifyTimeDuration("set-timeout 30s;", "30s", 30 * 1000);

        // Test minutes
        verifyTimeDuration("set-timeout 5m;", "5m", 5 * 60 * 1000);

        // Test hours
        verifyTimeDuration("set-timeout 1h;", "1h", 60 * 60 * 1000);

        // Test days
        verifyTimeDuration("set-timeout 2d;", "2d", 2 * 24 * 60 * 60 * 1000);

        // Test weeks
        verifyTimeDuration("set-timeout 1w;", "1w", 7 * 24 * 60 * 60 * 1000);

        // Test months (30 days)
        verifyTimeDuration("set-timeout 1mo;", "1mo", 30 * 24 * 60 * 60 * 1000);

        // Test years (365 days)
        verifyTimeDuration("set-timeout 1y;", "1y", 365 * 24 * 60 * 60 * 1000);
    }

    @Test
    public void testAlternateUnitNames() throws Exception {
        // Test seconds
        verifyTimeDuration("set-timeout 30sec;", "30sec", 30 * 1000);
        verifyTimeDuration("set-timeout 30second;", "30second", 30 * 1000);
        verifyTimeDuration("set-timeout 30seconds;", "30seconds", 30 * 1000);

        // Test minutes
        verifyTimeDuration("set-timeout 5min;", "5min", 5 * 60 * 1000);
        verifyTimeDuration("set-timeout 5minute;", "5minute", 5 * 60 * 1000);
        verifyTimeDuration("set-timeout 5minutes;", "5minutes", 5 * 60 * 1000);

        // Test hours
        verifyTimeDuration("set-timeout 1hr;", "1hr", 60 * 60 * 1000);
        verifyTimeDuration("set-timeout 1hour;", "1hour", 60 * 60 * 1000);
        verifyTimeDuration("set-timeout 1hours;", "1hours", 60 * 60 * 1000);

        // Test days
        verifyTimeDuration("set-timeout 2day;", "2day", 2 * 24 * 60 * 60 * 1000);
        verifyTimeDuration("set-timeout 2days;", "2days", 2 * 24 * 60 * 60 * 1000);

        // Test weeks
        verifyTimeDuration("set-timeout 1week;", "1week", 7 * 24 * 60 * 60 * 1000);
        verifyTimeDuration("set-timeout 1weeks;", "1weeks", 7 * 24 * 60 * 60 * 1000);

        // Test months
        verifyTimeDuration("set-timeout 1month;", "1month", 30 * 24 * 60 * 60 * 1000);
        verifyTimeDuration("set-timeout 1months;", "1months", 30 * 24 * 60 * 60 * 1000);

        // Test years
        verifyTimeDuration("set-timeout 1year;", "1year", 365 * 24 * 60 * 60 * 1000);
        verifyTimeDuration("set-timeout 1years;", "1years", 365 * 24 * 60 * 60 * 1000);
    }

    @Test
    public void testMixedCaseUnits() throws Exception {
        // Test mixed case units
        verifyTimeDuration("set-timeout 5S;", "5S", 5 * 1000);
        verifyTimeDuration("set-timeout 5M;", "5M", 5 * 60 * 1000);
        verifyTimeDuration("set-timeout 5H;", "5H", 5 * 60 * 60 * 1000);
    }

    @Test
    public void testTimeDurationWithWhitespace() throws Exception {
        // Test with spaces around number and unit
        verifyTimeDuration("set-timeout 5 h;", "5 h", 5 * 60 * 60 * 1000);
    }

    @Test
    public void testMultipleTimeDurationTokens() throws Exception {
        // Test parsing a directive with multiple TIME_DURATION arguments
        new GrammarWalker(new RecipeCompiler(), new NoOpDirectiveContext()).walk("set-timerange 5m 2h;",
                (command, tokenGroup) -> {
                    // Verify command name
                    Assert.assertEquals("set-timerange", command);

                    // Verify that we have three tokens: command and two TimeDuration tokens
                    Assert.assertEquals(3, tokenGroup.size());

                    // Verify first TimeDuration token
                    Token token1 = tokenGroup.get(1);
                    Assert.assertEquals(TokenType.TIME_DURATION, token1.type());
                    Assert.assertTrue(token1 instanceof TimeDuration);
                    TimeDuration duration1 = (TimeDuration) token1;
                    Assert.assertEquals("5m", duration1.value());
                    Assert.assertEquals(5 * 60 * 1000, duration1.getMilliseconds());

                    // Verify second TimeDuration token
                    Token token2 = tokenGroup.get(2);
                    Assert.assertEquals(TokenType.TIME_DURATION, token2.type());
                    Assert.assertTrue(token2 instanceof TimeDuration);
                    TimeDuration duration2 = (TimeDuration) token2;
                    Assert.assertEquals("2h", duration2.value());
                    Assert.assertEquals(2 * 60 * 60 * 1000, duration2.getMilliseconds());
                });
    }

    @Test(expected = Exception.class)
    public void testInvalidTimeDurationSyntax() throws Exception {
        // Test with invalid time duration syntax
        new GrammarWalker(new RecipeCompiler(), new NoOpDirectiveContext()).walk("set-timeout 5x;",
                (command, tokenGroup) -> {
                    // Should not reach here as an exception should be thrown
                    Assert.fail("Expected exception for invalid time unit");
                });
    }

    @Test(expected = Exception.class)
    public void testNonNumericTimeDuration() throws Exception {
        // Test with non-numeric time duration
        new GrammarWalker(new RecipeCompiler(), new NoOpDirectiveContext()).walk("set-timeout xs;",
                (command, tokenGroup) -> {
                    // Should not reach here as an exception should be thrown
                    Assert.fail("Expected exception for non-numeric time value");
                });
    }

    /**
     * Helper method to verify a TimeDuration token in a parsed directive.
     *
     * @param directive      The directive string to parse
     * @param expectedValue  Expected string value of the TimeDuration token
     * @param expectedMillis Expected number of milliseconds represented by the
     *                       token
     */
    private void verifyTimeDuration(String directive, String expectedValue, long expectedMillis) throws Exception {
        new GrammarWalker(new RecipeCompiler(), new NoOpDirectiveContext()).walk(directive,
                (command, tokenGroup) -> {
                    // Find the TimeDuration token
                    boolean found = false;
                    for (int i = 0; i < tokenGroup.size(); i++) {
                        Token token = tokenGroup.get(i);
                        if (token.type() == TokenType.TIME_DURATION) {
                            found = true;
                            Assert.assertEquals(expectedValue, token.value());
                            Assert.assertTrue(token instanceof TimeDuration);
                            TimeDuration duration = (TimeDuration) token;
                            Assert.assertEquals(expectedMillis, duration.getMilliseconds());
                            break;
                        }
                    }
                    Assert.assertTrue("TimeDuration token not found in parsed result", found);
                });
    }
}