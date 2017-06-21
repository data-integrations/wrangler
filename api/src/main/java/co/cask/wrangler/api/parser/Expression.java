package co.cask.wrangler.api.parser;

/**
 * Class description here.
 */
public class Expression implements Token {
  private String value;

  public Expression(String value) {
    this.value = value;
  }

  @Override
  public String get() {
    return value;
  }

  @Override
  public TokenType type() {
    return TokenType.EXPRESSION;
  }
}
