/*
 *  Copyright © 2021 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.utils;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Transformations with Decimal
 */
public class DecimalTransform {

  /**
   Add a value to a column
   */
  public static BigDecimal add(BigDecimal bd1, Object o) {
    BigDecimal bd2 = objectToBigDecimal(o);
    if (bd1 == null) {
      return bd2;
    }
    return bd1.add(bd2);
  }

  /**
   Subtract a value from a column
   */
  public static BigDecimal subtract(BigDecimal bd1, Object o) {
    BigDecimal bd2 = objectToBigDecimal(o);
    if (bd1 == null) {
      return bd2.negate();
    }
    return bd1.subtract(bd2);
  }

  /**
   Multiply the column by a given value
   */
  public static BigDecimal multiply(BigDecimal bd1, Object o) {
    BigDecimal bd2 = objectToBigDecimal(o);
    if (bd1 == null) {
      return BigDecimal.ZERO;
    }
    return bd1.multiply(bd2);
  }

  /**
   Divide the column by a given value and return the quotient
   */
  public static BigDecimal divideq(BigDecimal bd1, Object o) {
    BigDecimal bd2 = objectToBigDecimal(o);
    if (bd2.equals(BigDecimal.ZERO)) {
      return null;
    }
    if (bd1 == null) {
      return BigDecimal.ZERO;
    }
    return bd1.divide(bd2, BigDecimal.ROUND_HALF_EVEN);
  }

  /**
   Divide the column by a given value and return the remainder
   */
  public static BigDecimal divider(BigDecimal bd1, Object o) {
    BigDecimal bd2 = objectToBigDecimal(o);
    if (bd2.equals(BigDecimal.ZERO)) {
      return null;
    }
    if (bd1 == null) {
      return BigDecimal.ZERO;
    }
    return bd1.remainder(bd2);
  }

  /**
  Get the absolute value
   */
  public static BigDecimal abs(BigDecimal bd) {
    return bd.abs();
  }

  /**
   Get the precision of a decimal value
   */
  public static int precision(BigDecimal bd) {
    return bd.precision();
  }

  /**
   Get the scale of a decimal value
   */
  public static int scale(BigDecimal bd) {
    return bd.scale();
  }

  /**
   Get the unscaled value of a decimal value
   */
  public static BigInteger unscaled(BigDecimal bd) {
    return bd.unscaledValue();
  }

  /**
   Move the decimal point n places to the left
   */
  public static BigDecimal decimal_left(BigDecimal bd, int n) {
    int newScale = bd.scale() + n;
    bd = bd.setScale(newScale, BigDecimal.ROUND_UP);
    bd = bd.divide(BigDecimal.valueOf(Math.pow(10, n)), BigDecimal.ROUND_HALF_EVEN);
    return bd.stripTrailingZeros();
  }

  /**
   Move the decimal point n places to the right
   */
  public static BigDecimal decimal_right(BigDecimal bd, int n) {
    int newScale = Math.max(bd.scale() - n, 0);
    bd = bd.multiply(BigDecimal.valueOf(Math.pow(10, n)));
    bd = bd.setScale(newScale, BigDecimal.ROUND_DOWN);
    return bd;
  }

  /**
   Get the nth power of a decimal
   */
  public static BigDecimal pow(BigDecimal bd, int pow) {
    return bd.pow(pow);
  }

  /**
   Negate a decimal
   */
  public static BigDecimal negate(BigDecimal bd) {
    bd = bd.negate();
    if (bd.compareTo(BigDecimal.ZERO) == 0) {
      return BigDecimal.ZERO;
    }
    return bd;
  }

  /**
   Strip trailing zeros in a decimal
   */
  public static BigDecimal strip_zero(BigDecimal bd) {
    return bd.stripTrailingZeros();
  }

  /**
   Sign of a value
   */
  public static int sign(BigDecimal bd) {
    return Integer.compare(bd.compareTo(BigDecimal.ZERO), 0);
  }

  private static BigDecimal objectToBigDecimal(Object o) {
    if (o instanceof BigDecimal) {
      return (BigDecimal) o;
    } else if (o instanceof Integer) {
      return BigDecimal.valueOf((Integer) o);
    } else if (o instanceof Double) {
      return BigDecimal.valueOf((Double) o);
    } else {
      return null;
    }
  }
}
