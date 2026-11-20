/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
package io.cdap.wrangler.api.parser;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import io.cdap.wrangler.api.TokenGroup;

/**
 * Unit tests for the TokenGroup class.
 */
public class TokenGroupTest {

  @Test
  public void testAddAndSize() {
    TokenGroup tokenGroup = new TokenGroup();
    Token byteSizeToken = new MockToken(TokenType.BYTE_SIZE, "10KB");
    Token timeDurationToken = new MockToken(TokenType.TIME_DURATION, "5m");

    tokenGroup.add(byteSizeToken);
    tokenGroup.add(timeDurationToken);

    Assert.assertEquals(2, tokenGroup.size());
  }

  @Test
  public void testGetTokensByType() {
    TokenGroup tokenGroup = new TokenGroup();
    Token byteSizeToken1 = new MockToken(TokenType.BYTE_SIZE, "10KB");
    Token byteSizeToken2 = new MockToken(TokenType.BYTE_SIZE, "2MB");
    Token timeDurationToken = new MockToken(TokenType.TIME_DURATION, "5m");

    tokenGroup.add(byteSizeToken1);
    tokenGroup.add(byteSizeToken2);
    tokenGroup.add(timeDurationToken);

    List<Token> byteSizeTokens = tokenGroup.getTokensByType(TokenType.BYTE_SIZE);
    Assert.assertEquals(2, byteSizeTokens.size());
    Assert.assertEquals("10KB", byteSizeTokens.get(0).value());
    Assert.assertEquals("2MB", byteSizeTokens.get(1).value());

    List<Token> timeDurationTokens = tokenGroup.getTokensByType(TokenType.TIME_DURATION);
    Assert.assertEquals(1, timeDurationTokens.size());
    Assert.assertEquals("5m", timeDurationTokens.get(0).value());
  }

  @Test
  public void testContainsTokenType() {
    TokenGroup tokenGroup = new TokenGroup();
    Token byteSizeToken = new MockToken(TokenType.BYTE_SIZE, "10KB");

    tokenGroup.add(byteSizeToken);

    Assert.assertTrue(tokenGroup.containsTokenType(TokenType.BYTE_SIZE));
    Assert.assertFalse(tokenGroup.containsTokenType(TokenType.TIME_DURATION));
  }
}

//end of file