package co.cask.wrangler.api.parser;

import java.util.List;

/**
 * Class description here.
 */
public class BoolList implements Token {
  private List<Boolean> values;

  public BoolList(List<Boolean> value) {
    this.values = values;
  }

  @Override
  public List<Boolean> get() {
    return values;
  }

  @Override
  public TokenType type() {
    return TokenType.BOOLEAN_LIST;
  }
}
