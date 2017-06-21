package co.cask.wrangler.api.parser;

import co.cask.wrangler.api.LazyNumber;

import java.util.List;

/**
 * Class description here.
 */
public class NumericList implements Token {
  private final List<LazyNumber> numerics;

  public NumericList(List<LazyNumber> numerics) {
    this.numerics = numerics;
  }

  @Override
  public List<LazyNumber> get() {
    return numerics;
  }

  @Override
  public TokenType type() {
    return TokenType.NUMERIC_LIST;
  }
}
