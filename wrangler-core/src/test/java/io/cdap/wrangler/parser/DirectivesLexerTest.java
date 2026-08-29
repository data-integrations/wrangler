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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DirectivesLexerTest {

    @Test
    public void testByteSizeTokens() {
        String input = "1B 1KB 1MB 1GB 1TB 1PB";
        DirectivesLexer lexer = new DirectivesLexer(CharStreams.fromString(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        List<Token> tokenList = tokens.getTokens();
        
        // Remove EOF token
        tokenList = tokenList.subList(0, tokenList.size() - 1);
        
        Assert.assertEquals(6, tokenList.size());
        for (Token token : tokenList) {
            Assert.assertEquals(DirectivesLexer.BYTE_SIZE, token.getType());
        }
        
        // Test case insensitivity
        Assert.assertEquals("1B", tokenList.get(0).getText());
        Assert.assertEquals("1KB", tokenList.get(1).getText());
        Assert.assertEquals("1MB", tokenList.get(2).getText());
        Assert.assertEquals("1GB", tokenList.get(3).getText());
        Assert.assertEquals("1TB", tokenList.get(4).getText());
        Assert.assertEquals("1PB", tokenList.get(5).getText());
    }

    @Test
    public void testTimeDurationTokens() {
        String input = "1ns 1us 1ms 1s 1m 1h 1d";
        DirectivesLexer lexer = new DirectivesLexer(CharStreams.fromString(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        List<Token> tokenList = tokens.getTokens();
        
        // Remove EOF token
        tokenList = tokenList.subList(0, tokenList.size() - 1);
        
        Assert.assertEquals(7, tokenList.size());
        for (Token token : tokenList) {
            Assert.assertEquals(DirectivesLexer.TIME_DURATION, token.getType());
        }
        
        Assert.assertEquals("1ns", tokenList.get(0).getText());
        Assert.assertEquals("1us", tokenList.get(1).getText());
        Assert.assertEquals("1ms", tokenList.get(2).getText());
        Assert.assertEquals("1s", tokenList.get(3).getText());
        Assert.assertEquals("1m", tokenList.get(4).getText());
        Assert.assertEquals("1h", tokenList.get(5).getText());
        Assert.assertEquals("1d", tokenList.get(6).getText());
    }

    @Test
    public void testInvalidByteSizeTokens() {
        String input = "1XB 1YB";
        DirectivesLexer lexer = new DirectivesLexer(CharStreams.fromString(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        List<Token> tokenList = tokens.getTokens();
        
        // Remove EOF token
        tokenList = tokenList.subList(0, tokenList.size() - 1);
        
        // These should be parsed as identifiers, not BYTE_SIZE tokens
        for (Token token : tokenList) {
            Assert.assertNotEquals(DirectivesLexer.BYTE_SIZE, token.getType());
        }
    }

    @Test
    public void testInvalidTimeDurationTokens() {
        String input = "1xs 1ys";
        DirectivesLexer lexer = new DirectivesLexer(CharStreams.fromString(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        List<Token> tokenList = tokens.getTokens();
        
        // Remove EOF token
        tokenList = tokenList.subList(0, tokenList.size() - 1);
        
        // These should be parsed as identifiers, not TIME_DURATION tokens
        for (Token token : tokenList) {
            Assert.assertNotEquals(DirectivesLexer.TIME_DURATION, token.getType());
        }
    }
} 