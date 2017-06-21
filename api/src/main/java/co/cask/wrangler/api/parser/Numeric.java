package co.cask.wrangler.api.parser;

import co.cask.wrangler.api.LazyNumber;

/**
 * Class description here.
 */
public class Numeric implements Token {
  private final LazyNumber numeric;

  public Numeric(LazyNumber numeric) {
    this.numeric = numeric;
  }

  @Override
  public LazyNumber get() {
    return numeric;
  }

  @Override
  public TokenType type() {
    return TokenType.NUMERIC;
  }
}
