package co.cask.wrangler.api;

import java.io.ObjectStreamException;
import java.math.BigDecimal;

/**
 * This class holds a number value that is lazily converted to a specific number type
 *
 * @author Inderjeet Singh
 */
public final class LazyNumber extends Number {
  private final String value;

  public LazyNumber(String value) {
    this.value = value;
  }

  @Override
  public int intValue() {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      try {
        return (int) Long.parseLong(value);
      } catch (NumberFormatException nfe) {
        return new BigDecimal(value).intValue();
      }
    }
  }

  @Override
  public long longValue() {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return new BigDecimal(value).longValue();
    }
  }

  @Override
  public float floatValue() {
    return Float.parseFloat(value);
  }

  @Override
  public double doubleValue() {
    return Double.parseDouble(value);
  }

  @Override
  public String toString() {
    return value;
  }

  /**
   * If somebody is unlucky enough to have to serialize one of these, serialize
   * it as a BigDecimal so that they won't need Gson on the other side to
   * deserialize it.
   */
  private Object writeReplace() throws ObjectStreamException {
    return new BigDecimal(value);
  }
}