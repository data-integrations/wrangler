package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.TokenDefinition;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class UsageDefinitionTest {

  @Test
  public void testUsageDefinitionConstructor() {
    String keyword = "parse";
    int minArgs = 1;
    List<TokenDefinition> args = Collections.emptyList();

    // Fix: Use correct constructor
    UsageDefinition definition = new UsageDefinition(keyword, minArgs, args);

    // Fix: Use field access or define getter methods
    assertEquals("Keyword should match", keyword, definition.getKeyword());
    assertEquals("Minimum arguments should match", minArgs, definition.getMinimumArguments());
    assertEquals("Arguments list should match", args, definition.getArguments());
  }
}
