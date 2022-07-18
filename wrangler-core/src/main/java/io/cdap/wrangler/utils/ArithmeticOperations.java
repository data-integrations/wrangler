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
import io.cdap.wrangler.api.DirectiveExecutionException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Cross-column operations for arithmetic
 */
public class ArithmeticOperations {
  /**
   * Arithmetic operation - Add two columns.
   */
  public static Integer add(Integer x, Integer y) {
    if (x == null || y == null) {
      return null;
    }
    return x + y;
  }

  /**
   * Arithmetic operation - Add two columns.
   */
  public static Double add(Double x, Double y) {
    if (x == null || y == null) {
      return null;
    }
    return x + y;
  }

  /**
   * Arithmetic operation - Add two columns.
   */
  public static Float add(Float x, Float y) {
    if (x == null || y == null) {
      return null;
    }
    return x + y;
  }

  /**
   * Arithmetic operation - Add two columns.
   */
  public static BigDecimal add(BigDecimal x, BigDecimal y) {
    if (x == null || y == null) {
      return null;
    }
    return x.add(y).stripTrailingZeros();
  }

  /**
   * Arithmetic operation - Subtract a column from another column.
   */
  public static Integer minus(Integer x, Integer y) {
    if (x == null || y == null) {
      return null;
    }
    return x - y;
  }

  /**
   * Arithmetic operation - Subtract a column from another column.
   */
  public static Double minus(Double x, Double y) {
    if (x == null || y == null) {
      return null;
    }
    return x - y;
  }

  /**
   * Arithmetic operation - Subtract a column from another column.
   */
  public static Float minus(Float x, Float y) {
    if (x == null || y == null) {
      return null;
    }
    return x - y;
  }

  /**
   * Arithmetic operation - Subtract a column from another column.
   */
  public static BigDecimal minus(BigDecimal x, BigDecimal y) {
    if (x == null || y == null) {
      return null;
    }
    return x.subtract(y).stripTrailingZeros();
  }

  /**
   Arithmetic operation - Multiply a column with another column.
   */
  public static Integer multiply(Integer x, Integer y) {
    if (x == null || y == null) {
      return null;
    }
    return x * y;
  }

  /**
   * Arithmetic operation - Multiply a column with another column.
   */
  public static Double multiply(Double x, Double y) {
    if (x == null || y == null) {
      return null;
    }
    return x * y;
  }

  /**
   * Arithmetic operation - Multiply a column with another column.
   */
  public static Float multiply(Float x, Float y) {
    if (x == null || y == null) {
      return null;
    }
    return x * y;
  }

  /**
   * Arithmetic operation - Multiply a column with another column.
   */
  public static BigDecimal multiply(BigDecimal x, BigDecimal y) {
    if (x == null || y == null) {
      return null;
    }
    return x.multiply(y);
  }

  /**
   * Arithmetic operation - Divide a column by another and return the quotient.
   */
  public static Integer divideq(Integer x, Integer y) {
    if (x == null || y == null || y == 0) {
      return null;
    }
    return x / y;
  }

  /**
   * Arithmetic operation - Divide a column by another and return the quotient.
   */
  public static Double divideq(Double x, Double y) {
    if (x == null || y == null || y == 0) {
      return null;
    }
    return x / y;
  }

  /**
   * Arithmetic operation - Divide a column by another and return the quotient.
   */
  public static Float divideq(Float x, Float y) {
    if (x == null || y == null || y == 0) {
      return null;
    }
    return x / y;
  }

  /**
   * Arithmetic operation - Divide a column by another and return the quotient.
   */
  public static BigDecimal divideq(BigDecimal x, BigDecimal y) {
    if (x == null || y == null || y.equals(BigDecimal.valueOf(0))) {
      return null;
    }
    return x.divide(y, BigDecimal.ROUND_HALF_EVEN).stripTrailingZeros();
  }

  /**
   * Arithmetic operation - Divide a column by another and return the remainder.
   */
  public static Integer divider(Integer x, Integer y) {
    if (x == null || y == null || y == 0) {
      return null;
    }
    return x % y;
  }

  /**
   * Arithmetic operation - Divide a column by another and return the remainder.
   */
  public static Double divider(Double x, Double y) {
    if (x == null || y == null || y == 0) {
      return null;
    }
    return x % y;
  }

  /**
   * Arithmetic operation - Divide a column by another and return the remainder.
   */
  public static Float divider(Float x, Float y) {
    if (x == null || y == null || y == 0) {
      return null;
    }
    return x % y;
  }

  /**
   * Arithmetic operation - Divide a column by another and return the remainder.
   */
  public static BigDecimal divider(BigDecimal x, BigDecimal y) {
    if (x == null || y == null || y.equals(BigDecimal.valueOf(0))) {
      return null;
    }
    return x.remainder(y);
  }

  /**
   * Arithmetic operation - calculate LCM of two columns.
   */
  public static Integer lcm(Integer i1, Integer i2) {
    if (i1 == null || i2 == null) {
      return null;
    }
    BigDecimal bd1 = BigDecimal.valueOf(i1);
    BigDecimal bd2 = BigDecimal.valueOf(i2);

    return lcm(bd1, bd2).intValue();
  }

  /**
   * Arithmetic operation - calculate LCM of two columns.
   */
  public static Double lcm(Double d1, Double d2) {
    if (d1 == null || d2 == null) {
      return null;
    }
    BigDecimal bd1 = BigDecimal.valueOf(d1);
    BigDecimal bd2 = BigDecimal.valueOf(d2);
    return lcm(bd1, bd2).doubleValue();
  }

  /**
   * Arithmetic operation - calculate LCM of two columns.
   */
  public static Float lcm(Float f1, Float f2) {
    if (f1 == null || f2 == null) {
      return null;
    }
    BigDecimal bd1 = BigDecimal.valueOf(f1);
    BigDecimal bd2 = BigDecimal.valueOf(f2);
    return lcm(bd1, bd2).floatValue();
  }

  /**
   * Arithmetic operation - calculate LCM of two columns.
   */
  public static BigDecimal lcm(BigDecimal bd1, BigDecimal bd2) {
    if (bd1 == null || bd2 == null) {
      return null;
    }
    BigDecimal pow = BigDecimal.valueOf(Math.pow(10, Math.max(bd1.scale(), bd2.scale())));
    BigInteger val1 = bd1.multiply(pow).toBigInteger();
    BigInteger val2 = bd2.multiply(pow).toBigInteger();
    BigDecimal absProduct = new BigDecimal(val1.multiply(val2).abs())
      .setScale(Math.min(bd1.scale(), bd2.scale()), BigDecimal.ROUND_HALF_EVEN);
    BigDecimal gcd = new BigDecimal(val1.gcd(val2));
    if (gcd.compareTo(BigDecimal.ZERO) == 0) {
      return BigDecimal.ZERO;
    }
    return absProduct.divide(gcd.multiply(pow), BigDecimal.ROUND_HALF_EVEN);
  }

  /**
   * Arithmetic operation - Check if a value is equal to another column.
   */
  public static Boolean equal(Integer i1, Integer i2) {
    if (i1 == null || i2 == null) {
      return null;
    }
    return i1.equals(i2);
  }

  /**
   * Arithmetic operation - Check if a value is equal to another column.
   */
  public static Boolean equal(Double d1, Double d2) {
    if (d1 == null || d2 == null) {
      return null;
    }
    return d1.equals(d2);
  }

  /**
   * Arithmetic operation - Check if a value is equal to another column.
   */
  public static Boolean equal(Float f1, Float f2) {
    if (f1 == null || f2 == null) {
      return null;
    }
    return f1.equals(f2);
  }

  /**
   * Arithmetic operation - Check if a value is equal to another column.
   */
  public static Boolean equal(BigDecimal bd1, BigDecimal bd2) {
    if (bd1 == null || bd2 == null) {
      return null;
    }
    return bd1.equals(bd2);
  }

  /**
   * Arithmetic operation - Find the maximum of two columns.
   */
  public static Integer max(Integer i1, Integer i2) {
    if (i1 == null || i2 == null) {
      return null;
    }
    return Math.max(i1, i2);
  }

  /**
   * Arithmetic operation - Find the maximum of two columns.
   */
  public static Double max(Double d1, Double d2) {
    if (d1 == null || d2 == null) {
      return null;
    }
    return Math.max(d1, d2);
  }

  /**
   * Arithmetic operation - Find the maximum of two columns.
   */
  public static Float max(Float f1, Float f2) {
    if (f1 == null || f2 == null) {
      return null;
    }
    return Math.max(f1, f2);
  }

  /**
   * Arithmetic operation - Find the maximum of two columns.
   */
  public static BigDecimal max(BigDecimal bd1, BigDecimal bd2) {
    if (bd1 == null || bd2 == null) {
      return null;
    }
    return bd1.max(bd2);
  }

  /**
   * Arithmetic operation - Find the minimum of two columns.
   */
  public static Integer min(Integer i1, Integer i2) {
    if (i1 == null || i2 == null) {
      return null;
    }
    return Math.min(i1, i2);
  }

  /**
   * Arithmetic operation - Find the minimum of two columns.
   */
  public static Double min(Double d1, Double d2) {
    if (d1 == null || d2 == null) {
      return null;
    }
    return Math.min(d1, d2);
  }

  /**
   * Arithmetic operation - Find the minimum of two columns.
   */
  public static Float min(Float f1, Float f2) {
    if (f1 == null || f2 == null) {
      return null;
    }
    return Math.min(f1, f2);
  }

  /**
   * Arithmetic operation - Find the minimum of two columns.
   */
  public static BigDecimal min(BigDecimal bd1, BigDecimal bd2) {
    if (bd1 == null || bd2 == null) {
      return null;
    }
    return bd1.min(bd2);
  }

  /**
   * Arithmetic operation - calculate average of any number of columns.
   * Returns Number - BigDecimal if any column is of type BigDecimal, double otherwise.
   * HALF_EVEN rounding mode chosen for high precision in the long run.
   */
  public static Number average(Number... nums) throws DirectiveExecutionException {
    // Note that this algorithm avoids overflow
    Number avg = 0.0;
    int t = 1;
    boolean allNull = true;
    boolean containsBigDecimal = false;
    for (Number num : nums) {
      // Check for null input
      if (num == null) {
        continue;
      } else {
        allNull = false;
      }
      // Check for invalid input
      if (num instanceof Byte) {
        throw new DirectiveExecutionException("Input cannot be of type 'Byte'.");
      }
      // Switch to BigDecimal computation if necessary
      if (!containsBigDecimal && num instanceof BigDecimal) {
        if (t > 1) {
          avg = BigDecimal.valueOf((double) avg);
        } else {
          avg = BigDecimal.ZERO;
        }
        containsBigDecimal = true;
      }
      // Compute average
      if (containsBigDecimal) {
        BigDecimal numer;
        if (num instanceof BigDecimal) {
          numer = ((BigDecimal) num).subtract((BigDecimal) avg);
        } else {
          numer = BigDecimal.valueOf(num.doubleValue()).subtract((BigDecimal) avg);
        }
        BigDecimal denom = BigDecimal.valueOf(t);
        avg = ((BigDecimal) avg).add(numer.divide(denom, RoundingMode.HALF_EVEN));
      } else {
        avg = (double) avg + (num.doubleValue() - (double) avg) / t;
      }
      t++;
    }
    if (allNull) {
      return null;
    }
    return avg;
  }
}
