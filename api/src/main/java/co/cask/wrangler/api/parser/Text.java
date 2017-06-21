package co.cask.wrangler.api.parser;

/**
 * Class description here.
 */
public class Text implements Token {
  private String value;

  public Text(String value) {
    this.value = value;
  }

  @Override
  public String get() {
    return value;
  }

  @Override
  public TokenType type() {
    return TokenType.TEXT;
  }
}
