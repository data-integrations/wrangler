package io.cdap.wrangler.api.parser;

import java.util.List;

public class UsageDefinition {
  private final String keyword;
  private final int minimumArguments;
  private final List<TokenDefinition> arguments;

  public UsageDefinition(String keyword, int minimumArguments, List<TokenDefinition> arguments) {
    this.keyword = keyword;
    this.minimumArguments = minimumArguments;
    this.arguments = arguments;
  }

  public String getKeyword() {
    return keyword;
  }

  public int getMinimumArguments() {
    return minimumArguments;
  }

  public List<TokenDefinition> getArguments() {
    return arguments;
  }
}
