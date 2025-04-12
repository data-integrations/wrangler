/*
 * Copyright © 2016-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.TokenDefinition;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit test for {@link UsageDefinition}
 */
public class UsageDefinitionTest {

  @Test
  public void testUsageDefinitionBuilder() {
    UsageDefinition definition = UsageDefinition.builder("parse-as-csv")
      .define("body", TokenType.COLUMN_NAME)
      .define("delimiter", TokenType.TEXT)
      .build();

    assertEquals("parse-as-csv", definition.getDirectiveName());

    List<TokenDefinition> tokens = definition.getTokens();
    assertEquals(2, tokens.size());
    assertEquals("body", tokens.get(0).name());
    assertEquals(TokenType.COLUMN_NAME, tokens.get(0).type());
    assertEquals("delimiter", tokens.get(1).name());
    assertEquals(TokenType.TEXT, tokens.get(1).type());
  }

  @Test
  public void testKeywordOnlyDefinition() {
    UsageDefinition definition = UsageDefinition.builder("split-to-columns").build();
    assertEquals("split-to-columns", definition.getDirectiveName());
    assertTrue(definition.getTokens().isEmpty());
  }

  @Test
  public void testByteSizeAndTimeDurationTokens() {
    UsageDefinition definition = UsageDefinition.builder("memory-config")
      .define("heap", TokenType.BYTE_SIZE)
      .define("timeout", TokenType.TIME_DURATION)
      .build();

    assertEquals("memory-config", definition.getDirectiveName());

    List<TokenDefinition> tokens = definition.getTokens();
    assertEquals(2, tokens.size());

    assertEquals("heap", tokens.get(0).name());
    assertEquals(TokenType.BYTE_SIZE, tokens.get(0).type());

    assertEquals("timeout", tokens.get(1).name());
    assertEquals(TokenType.TIME_DURATION, tokens.get(1).type());
  }
}
