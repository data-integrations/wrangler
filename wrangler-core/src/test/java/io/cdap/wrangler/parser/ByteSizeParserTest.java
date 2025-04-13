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
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for ByteSize token parsing in the wrangler grammar.
 */
public class ByteSizeParserTest {

    /**
     * Helper to create a parser instance for testing
     */
    private RecipeParser createParser(String recipe) {
        CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(
                SystemDirectiveRegistry.INSTANCE);
        return new GrammarBasedParser(Contexts.SYSTEM, recipe, registry);
    }

    @Test
    public void testBasicByteSizeParsing() throws Exception {
        // Parse a simple directive that would take a BYTE_SIZE argument
        RecipeParser parser = createParser("set-column width 5MB;");
        List<io.cdap.wrangler.api.Directive> directives = parser.parse();

        // This verifies that the directive parsed successfully
        Assert.assertEquals(1, directives.size());

        // Use the GrammarWalker to access the tokens directly for verification
        new GrammarWalker(new RecipeCompiler(), new NoOpDirectiveContext()).walk("set-column width 5MB;",
                (command, tokenGroup) -> {
                    // Verify command name
                    Assert.assertEquals("set-column", command);

                    // Verify that the ByteSize token exists and is correctly parsed
                    boolean foundByteSize = false;
                    for (int i = 0; i < tokenGroup.size(); i++) {
                        Token token = tokenGroup.get(i);
                        if (token.type() == TokenType.BYTE_SIZE) {
                            foundByteSize = true;
                            Assert.assertEquals("5MB", token.value());
                            Assert.assertTrue(token instanceof ByteSize);
                            ByteSize byteSize = (ByteSize) token;
                            Assert.assertEquals(5 * 1024 * 1024, byteSize.getBytes());
                            break;
                        }
                    }
                    Assert.assertTrue("ByteSize token not found in parsed result", foundByteSize);
                });
    }

    @Test
    public void testByteSizeVariations() throws Exception {
        // Test KB
        verifyByteSize("set-column size 10kb;", "10kb", 10 * 1024);

        // Test MB
        verifyByteSize("set-column size 2.5MB;", "2.5MB", (long) (2.5 * 1024 * 1024));

        // Test GB
        verifyByteSize("set-column size 1gb;", "1gb", 1 * 1024 * 1024 * 1024);

        // Test TB
        verifyByteSize("set-column size 0.5TB;", "0.5TB", (long) (0.5 * 1024 * 1024 * 1024 * 1024));

        // Test PB
        verifyByteSize("set-column size 0.01PB;", "0.01PB", (long) (0.01 * 1024 * 1024 * 1024 * 1024 * 1024));
    }

    @Test
    public void testMixedCaseUnits() throws Exception {
        // Test mixed case units
        verifyByteSize("set-column size 1Kb;", "1Kb", 1 * 1024);

        verifyByteSize("set-column size 1mB;", "1mB", 1 * 1024 * 1024);

        verifyByteSize("set-column size 1Gb;", "1Gb", 1 * 1024 * 1024 * 1024);
    }

    @Test
    public void testByteSizeWithWhitespace() throws Exception {
        // Test with spaces around number and unit
        verifyByteSize("set-column size 5 KB;", "5 KB", 5 * 1024);
    }

    @Test
    public void testMultipleByteSizeTokens() throws Exception {
        // Test parsing a directive with multiple BYTE_SIZE arguments
        new GrammarWalker(new RecipeCompiler(), new NoOpDirectiveContext()).walk("set-range 1KB 10MB;",
                (command, tokenGroup) -> {
                    // Verify command name
                    Assert.assertEquals("set-range", command);

                    // Find all ByteSize tokens and verify them
                    int byteSizeTokensFound = 0;
                    long[] expectedBytes = { 1 * 1024, 10 * 1024 * 1024 };
                    String[] expectedValues = { "1KB", "10MB" };

                    for (int i = 0; i < tokenGroup.size(); i++) {
                        Token token = tokenGroup.get(i);
                        if (token.type() == TokenType.BYTE_SIZE) {
                            Assert.assertTrue("Index out of bounds: " + byteSizeTokensFound,
                                    byteSizeTokensFound < expectedBytes.length);
                            Assert.assertEquals(expectedValues[byteSizeTokensFound], token.value());
                            Assert.assertTrue(token instanceof ByteSize);
                            ByteSize byteSize = (ByteSize) token;
                            Assert.assertEquals(expectedBytes[byteSizeTokensFound], byteSize.getBytes());
                            byteSizeTokensFound++;
                        }
                    }

                    Assert.assertEquals("Expected number of ByteSize tokens not found", expectedBytes.length,
                            byteSizeTokensFound);
                });
    }

    @Test(expected = Exception.class)
    public void testInvalidByteSizeSyntax() throws Exception {
        // Test with invalid byte size syntax
        new GrammarWalker(new RecipeCompiler(), new NoOpDirectiveContext()).walk("set-column size 5K;",
                (command, tokenGroup) -> {
                    // Should not reach here as an exception should be thrown
                    Assert.fail("Expected exception for invalid byte size unit");
                });
    }

    @Test(expected = Exception.class)
    public void testNonNumericByteSize() throws Exception {
        // Test with non-numeric byte size
        new GrammarWalker(new RecipeCompiler(), new NoOpDirectiveContext()).walk("set-column size xMB;",
                (command, tokenGroup) -> {
                    // Should not reach here as an exception should be thrown
                    Assert.fail("Expected exception for non-numeric byte size value");
                });
    }

    /**
     * Helper method to verify a ByteSize token in the list of tokens.
     *
     * @param directive     The directive string to parse
     * @param expectedValue Expected string value of the ByteSize token
     * @param expectedBytes Expected number of bytes represented by the token
     */
    private void verifyByteSize(String directive, String expectedValue, long expectedBytes) throws Exception {
        new GrammarWalker(new RecipeCompiler(), new NoOpDirectiveContext()).walk(directive,
                (command, tokenGroup) -> {
                    boolean found = false;
                    for (int i = 0; i < tokenGroup.size(); i++) {
                        Token token = tokenGroup.get(i);
                        if (token.type() == TokenType.BYTE_SIZE) {
                            found = true;
                            Assert.assertEquals(expectedValue, token.value());
                            Assert.assertTrue(token instanceof ByteSize);
                            ByteSize byteSize = (ByteSize) token;
                            Assert.assertEquals(expectedBytes, byteSize.getBytes());
                            break;
                        }
                    }
                    Assert.assertTrue("ByteSize token not found in parsed result", found);
                });
    }
}