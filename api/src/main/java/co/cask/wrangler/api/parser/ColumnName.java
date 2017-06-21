package co.cask.wrangler.api.parser;

/**
 * Class description here.
 */
public class ColumnName implements Token {
  private String value;

  public ColumnName(String value) {
    this.value = value;
  }

  @Override
  public String get() {
    return value;
  }

  @Override
  public TokenType type() {
    return TokenType.COLUMN_NAME;
  }
}
