package co.cask.wrangler.api.parser;

/**
 * Class description here.
 */
public class DirectiveName implements Token {
  private String value;

  public DirectiveName(String value) {
    this.value = value;
  }

  @Override
  public String get() {
    return value;
  }

  @Override
  public TokenType type() {
    return TokenType.DIRECTIVE_NAME;
  }
}
