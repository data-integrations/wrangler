package co.cask.wrangler.expression;

/**
 * Class description here.
 */
public final class ELResult {
  private final Object value;

  public ELResult(Object value) {
    this.value = value;
  }

  public Object getObject() {
    return value;
  }

  public Boolean getBoolean() {
    if(value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      Boolean val = Boolean.parseBoolean((String) value);
      return val;
    }
    return null;
  }

  public Integer getInteger() {
    if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Long) {
      return ((Long) value).intValue();
    } else if (value instanceof Short) {
      return ((Short) value).intValue();
    } else if (value instanceof Double) {
      return ((Double) value).intValue();
    } else if (value instanceof Float) {
      return ((Float) value).intValue();
    }
    return null;
  }
}
