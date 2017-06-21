package co.cask.wrangler.api.parser;

/**
 * Class description here.
 */
public class Bool implements Token {
  private Boolean value;

  public Bool(Boolean value) {
    this.value = value;
  }

  @Override
  public Boolean get() {
    return value;
  }

  @Override
  public TokenType type() {
    return TokenType.BOOLEAN;
  }
}
