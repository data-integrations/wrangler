package co.cask.wrangler.api.parser;

import java.util.List;

/**
 * Class description here.
 */
public class TextList implements Token {
  private List<String> values;

  public TextList(List<String> value) {
    this.values = values;
  }

  @Override
  public List<String> get() {
    return values;
  }

  @Override
  public TokenType type() {
    return TokenType.TEXT_LIST;
  }
}
