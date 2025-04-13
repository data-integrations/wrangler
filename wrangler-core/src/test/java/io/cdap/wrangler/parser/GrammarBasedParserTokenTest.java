/*
 * Copyright © 2023-2025 Cask Data, Inc.
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

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.RecipeSymbol;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for new token types (BYTE_SIZE and TIME_DURATION) in the GrammarBasedParser
 */
public class GrammarBasedParserTokenTest {

  @Test
  public void testByteSizeToken() throws Exception {
    // Test that a ByteSize token is correctly recognized in a recipe
    String[] recipe = new String[] {
      "set-column :size 10KB"
    };
    
    // Parse the recipe
    RecipeParser parser = TestingRig.parse(recipe);
    RecipeSymbol symbol = parser.getRecipeSymbol();
    
    // Verify the symbol contains the ByteSize token
    List<TokenGroup> tokenGroups = symbol.getTokenGroups();
    Assert.assertEquals(1, tokenGroups.size());
    
    // The third token should be the ByteSize value
    Token byteSizeToken = tokenGroups.get(0).getToken(2);
    Assert.assertNotNull(byteSizeToken);
    Assert.assertEquals(TokenType.BYTE_SIZE, byteSizeToken.type());
    Assert.assertTrue(byteSizeToken instanceof ByteSize);
    
    // Verify the ByteSize value
    ByteSize byteSize = (ByteSize) byteSizeToken;
    Assert.assertEquals(10 * 1024, byteSize.getBytes());
  }

  @Test
  public void testTimeDurationToken() throws Exception {
    // Test that a TimeDuration token is correctly recognized in a recipe
    String[] recipe = new String[] {
      "set-column :duration 500ms"
    };
    
    // Parse the recipe
    RecipeParser parser = TestingRig.parse(recipe);
    RecipeSymbol symbol = parser.getRecipeSymbol();
    
    // Verify the symbol contains the TimeDuration token
    List<TokenGroup> tokenGroups = symbol.getTokenGroups();
    Assert.assertEquals(1, tokenGroups.size());
    
    // The third token should be the TimeDuration value
    Token timeDurationToken = tokenGroups.get(0).getToken(2);
    Assert.assertNotNull(timeDurationToken);
    Assert.assertEquals(TokenType.TIME_DURATION, timeDurationToken.type());
    Assert.assertTrue(timeDurationToken instanceof TimeDuration);
    
    // Verify the TimeDuration value
    TimeDuration timeDuration = (TimeDuration) timeDurationToken;
    Assert.assertEquals(500 * 1_000_000, timeDuration.getNanoseconds());
  }

  @Test
  public void testMixedTokens() throws Exception {
    // Test a recipe with both ByteSize and TimeDuration tokens
    String[] recipe = new String[] {
      "set-column :size 5MB",
      "set-column :duration 2s",
      "set-column :mixed_size 1.5GB",
      "set-column :mixed_duration 30min"
    };
    
    // Parse the recipe
    RecipeParser parser = TestingRig.parse(recipe);
    RecipeSymbol symbol = parser.getRecipeSymbol();
    
    // Verify the symbol contains the expected number of token groups
    List<TokenGroup> tokenGroups = symbol.getTokenGroups();
    Assert.assertEquals(4, tokenGroups.size());
    
    // Verify each token group has the expected type for the third token
    TokenGroup group1 = tokenGroups.get(0);
    Assert.assertEquals(TokenType.BYTE_SIZE, group1.getToken(2).type());
    Assert.assertEquals(5 * 1024 * 1024, ((ByteSize) group1.getToken(2)).getBytes());
    
    TokenGroup group2 = tokenGroups.get(1);
    Assert.assertEquals(TokenType.TIME_DURATION, group2.getToken(2).type());
    Assert.assertEquals(2 * 1_000_000_000, ((TimeDuration) group2.getToken(2)).getNanoseconds());
    
    TokenGroup group3 = tokenGroups.get(2);
    Assert.assertEquals(TokenType.BYTE_SIZE, group3.getToken(2).type());
    Assert.assertEquals(1.5 * 1024 * 1024 * 1024, ((ByteSize) group3.getToken(2)).getBytes(), 1.0);
    
    TokenGroup group4 = tokenGroups.get(3);
    Assert.assertEquals(TokenType.TIME_DURATION, group4.getToken(2).type());
    Assert.assertEquals(30 * 60 * 1_000_000_000L, ((TimeDuration) group4.getToken(2)).getNanoseconds());
  }

  @Test
  public void testAggregateStatsDirective() throws Exception {
    // Test that the AggregateStats directive correctly recognizes ByteSize and TimeDuration tokens as arguments
    String[] recipe = new String[] {
      "aggregate-stats :data_size :response_time total_size total_time 'total' 'MB' 's'"
    };
    
    // This should parse without exception
    TestingRig.parse(recipe);
  }
}