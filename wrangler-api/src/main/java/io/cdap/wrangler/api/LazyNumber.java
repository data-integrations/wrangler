/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api;

import java.math.BigDecimal;

/**
 * This class holds a number value that is lazily converted to a specific number type.
 * The value is stored as a string and only converted to a number when requested.
 * This class is immutable and thread-safe.
 */
public final class LazyNumber extends Number {
  /**
   * The string representation of the number.
   */
  private String value;

  /**
   * Constructs a new LazyNumber instance.
   * @param value The string representation of the number
   */
  public LazyNumber(String value) {
    this.value = value;
  }

  /**
   * Returns the value of the specified number as an {@code int}.
   * This may involve rounding or truncation.
   *
   * @return the numeric value represented by this object after conversion
   *         to type {@code int}
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
   * Returns the value of the specified number as a {@code long}.
   * This may involve rounding or truncation.
   *
   * @return the numeric value represented by this object after conversion
   *         to type {@code long}
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
   * Returns the value of the specified number as a {@code float}.
   * This may involve rounding.
   *
   * @return the numeric value represented by this object after conversion
   *         to type {@code float}
   */
  @Override
  public float floatValue() {
    try {
      return Float.parseFloat(value);
    } catch (NumberFormatException e) {
      return new BigDecimal(value).floatValue();
    }
  }

  /**
   * Returns the value of the specified number as a {@code double}.
   * This may involve rounding.
   *
   * @return the numeric value represented by this object after conversion
   *         to type {@code double}
   */
  @Override
  public double doubleValue() {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      return new BigDecimal(value).doubleValue();
    }
  }

  /**
   * Returns the string representation of this number.
   *
   * @return the string representation of this number
   */
  @Override
  public String toString() {
    return value;
  }
}
