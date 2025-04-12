/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link GrammarBasedParser}
 */
public class GrammarBasedParserTest {
  
  // Existing test methods...
  
  // Add new tests for byte size and time duration parsing
  
  @Test
  public void testByteSizeParsing() throws Exception {
    GrammarBasedParser parser = new GrammarBasedParser();
    
    // Test parsing directives with byte size tokens
    String directive = "aggregate-stats :data_size :response_time :total_size :total_time 10KB s";
    List<Token> tokens = parser.parse(directive);
    
    boolean foundByteSize = false;
    for (Token token : tokens) {
      if (token.type() == TokenType.BYTE_SIZE) {
        foundByteSize = true;
        ByteSize byteSize = (ByteSize) token;
        Assert.assertEquals("10KB", byteSize.value());
        Assert.assertEquals(10240L, byteSize.getBytes());
      }
    }
    Assert.assertTrue("ByteSize token not found", foundByteSize);
    
    // Test another byte size token
    directive = "some-directive :column1 1.5MB";
    tokens = parser.parse(directive);
    
    foundByteSize = false;
    for (Token token : tokens) {
      if (token.type() == TokenType.BYTE_SIZE) {
        foundByteSize = true;
        ByteSize byteSize = (ByteSize) token;
        Assert.assertEquals("1.5MB", byteSize.value());
        Assert.assertEquals((long) (1.5 * 1024 * 1024), byteSize.getBytes());
      }
    }
    Assert.assertTrue("ByteSize token not found", foundByteSize);
  }
  
  @Test
  public void testTimeDurationParsing() throws Exception {
    GrammarBasedParser parser = new GrammarBasedParser();
    
    // Test parsing directives with time duration tokens
    String directive = "aggregate-stats :data_size :response_time :total_size :total_time KB 100ms";
    List<Token> tokens = parser.parse(directive);
    
    boolean foundTimeDuration = false;
    for (Token token : tokens) {
      if (token.type() == TokenType.TIME_DURATION) {
        foundTimeDuration = true;
        TimeDuration timeDuration = (TimeDuration) token;
        Assert.assertEquals("100ms", timeDuration.value());
        Assert.assertEquals(100_000_000L, timeDuration.getNanoseconds());
      }
    }
    Assert.assertTrue("TimeDuration token not found", foundTimeDuration);
    
    // Test another time duration token
    directive = "some-directive :column1 2.5s";
    tokens = parser.parse(directive);
    
    foundTimeDuration = false;
    for (Token token : tokens) {
      if (token.type() == TokenType.TIME_DURATION) {
        foundTimeDuration = true;
        TimeDuration timeDuration = (TimeDuration) token;
        Assert.assertEquals("2.5s", timeDuration.value());
        Assert.assertEquals((long) (2.5 * 1_000_000_000), timeDuration.getNanoseconds());
      }
    }
    Assert.assertTrue("TimeDuration token not found", foundTimeDuration);
  }
  
  @Test
  public void testAggregateStatsDirectiveParsing() throws Exception {
    GrammarBasedParser parser = new GrammarBasedParser();
    
    // Test parsing of the aggregate-stats directive
    String directive = "aggregate-stats :data_size :response_time :total_size_mb :total_time_sec";
    List<Token> tokens = parser.parse(directive);
    
    // Verify directive name
    Assert.assertEquals(TokenType.DIRECTIVE_NAME, tokens.get(0).type());
    Assert.assertEquals("aggregate-stats", tokens.get(0).value());
    
    // Verify column names
    Assert.assertEquals(TokenType.COLUMN_NAME, tokens.get(1).type());
    Assert.assertEquals(":data_size", tokens.get(1).value());
    
    Assert.assertEquals(TokenType.COLUMN_NAME, tokens.get(2).type());
    Assert.assertEquals(":response_time", tokens.get(2).value());
    
    Assert.assertEquals(TokenType.COLUMN_NAME, tokens.get(3).type());
    Assert.assertEquals(":total_size_mb", tokens.get(3).value());
    
    Assert.assertEquals(TokenType.COLUMN_NAME, tokens.get(4).type());
    Assert.assertEquals(":total_time_sec", tokens.get(4).value());
  }
  
  @Test
  public void testAggregateStatsWithUnits() throws Exception {
    GrammarBasedParser parser = new GrammarBasedParser();
    
    // Test parsing of aggregate-stats with custom units
    String directive = "aggregate-stats :data_size :response_time :total_size :total_time GB m";
    List<Token> tokens = parser.parse(directive);
    
    // Verify directive name and column arguments
    Assert.assertEquals(TokenType.DIRECTIVE_NAME, tokens.get(0).type());
    Assert.assertEquals("aggregate-stats", tokens.get(0).value());
    
    // Verify unit arguments
    Assert.assertEquals(TokenType.IDENTIFIER, tokens.get(5).type());
    Assert.assertEquals("GB", tokens.get(5).value());
    
    Assert.assertEquals(TokenType.IDENTIFIER, tokens.get(6).type());
    Assert.assertEquals("m", tokens.get(6).value());
  }
}
