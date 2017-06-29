package co.cask.wrangler.api;

import java.math.BigDecimal;

/**
 * This class holds a number value that is lazily converted to a specific number type
 */
public final class LazyNumber extends Number {
  private final String value;

  public LazyNumber(String value) {
    this.value = value;
  }

  /**
   * Returns the value of the specified number as an <code>int</code>.
   * This may involve rounding or truncation.
   *
   * @return  the numeric value represented by this object after conversion
   *          to type <code>int</code>.
   */
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

  /**
   * Returns the value of the specified number as a <code>long</code>.
   * This may involve rounding or truncation.
   *
   * @return  the numeric value represented by this object after conversion
   *          to type <code>long</code>.
   */
  @Override
  public long longValue() {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return new BigDecimal(value).longValue();
    }
  }

  /**
   * Returns the value of the specified number as a <code>float</code>.
   * This may involve rounding.
   *
   * @return  the numeric value represented by this object after conversion
   *          to type <code>float</code>.
   */
  @Override
  public float floatValue() {
    return Float.parseFloat(value);
  }

  /**
   * Returns the value of the specified number as a <code>double</code>.
   * This may involve rounding.
   *
   * @return  the numeric value represented by this object after conversion
   *          to type <code>double</code>.
   */
  @Override
  public double doubleValue() {
    return Double.parseDouble(value);
  }

  @Override
  public String toString() {
    return value;
  }
}