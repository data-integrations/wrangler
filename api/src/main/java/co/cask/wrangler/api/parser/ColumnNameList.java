package co.cask.wrangler.api.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description here.
 */
public class ColumnNameList implements Token {
  private List<String> values = new ArrayList<>();

  public ColumnNameList(List<String> values) {
    this.values = values;
  }

  @Override
  public List<String> get() {
    return values;
  }

  @Override
  public TokenType type() {
    return TokenType.COLUMN_NAME_LIST;
  }
}
