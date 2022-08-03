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
  // The Number types upon which arithmetic operations can be performed, in order of increasing generality
  private enum NumberType {
    SHORT(0),
    INTEGER(1),
    LONG(2),
    FLOAT(3),
    DOUBLE(4),
    BIGDECIMAL(5);

    private final int priority;

    NumberType(int priority) {
      this.priority = priority;
    }

    public int getPriority() {
      return priority;
    }
  }

  /**
   * Return the "largest" (e.g. most general) type of the given numbers.
   * Used for operations applied to columns of multiple types, in order to determine the type of the output column.
   * The order of increasing generality is Short < Integer < Long < Float < Double < BigDecimal.
   * Returns null if any input is null.
   * Throws an error if any input of type Byte is given (this is not currently supported for arithmetic ops).
   */
  private static NumberType getOutputType(Number... nums) throws DirectiveExecutionException {
    NumberType outputType = NumberType.SHORT;
    for (Number num : nums) {
      if (num instanceof Byte) {
        throw new DirectiveExecutionException("Input cannot be of type 'Byte'.");
      } else if (num == null) {
        return null;
      } else if (num instanceof Integer && NumberType.INTEGER.getPriority() > outputType.getPriority()) {
        outputType = NumberType.INTEGER;
      } else if (num instanceof Long && NumberType.LONG.getPriority() > outputType.getPriority()) {
        outputType = NumberType.LONG;
      } else if (num instanceof Float && NumberType.FLOAT.getPriority() > outputType.getPriority()) {
        outputType = NumberType.FLOAT;
      } else if (num instanceof Double && NumberType.DOUBLE.getPriority() > outputType.getPriority()) {
        outputType = NumberType.DOUBLE;
      } else if (num instanceof BigDecimal) {
        outputType = NumberType.BIGDECIMAL;
      }
    }
    return outputType;
  }

  /**
   * Utility function to convert a number type to BigDecimal
   */
  private static BigDecimal numberToBigDecimal(Number num) {
    return num instanceof BigDecimal ? (BigDecimal) num : new BigDecimal(num.toString());
  }

  /**
   * Arithmetic operation - Find the sum of any number of columns, of any valid types.
   * Output type is most general of input types.
   * Returns null if any input value is null.
   */
  public static Number add(Number... nums) throws DirectiveExecutionException {
    NumberType outputType = getOutputType(nums);
    if (outputType == null) {
      return null;
    }
    switch (outputType) {
      case SHORT:
        short shortSum = 0;
        for (Number num : nums) {
          shortSum += num.shortValue();
        }
        return shortSum;
      case INTEGER:
        int intSum = 0;
        for (Number num : nums) {
          intSum += num.intValue();
        }
        return intSum;
      case LONG:
        long longSum = 0;
        for (Number num : nums) {
          longSum += num.longValue();
        }
        return longSum;
      case FLOAT:
        float floatSum = 0;
        for (Number num : nums) {
          floatSum += num.floatValue();
        }
        return floatSum;
      case DOUBLE:
        double doubleSum = 0;
        for (Number num : nums) {
          doubleSum += num.doubleValue();
        }
        return doubleSum;
      default:
        BigDecimal bdSum = BigDecimal.valueOf(0);
        for (Number num : nums) {
          bdSum = bdSum.add(numberToBigDecimal(num));
        }
        return bdSum.stripTrailingZeros();
    }
  }

  /**
   * Arithmetic operation - subtract a column from another column.
   * Output type is most general of input types.
   * Returns null if any input value is null.
   */
  public static Number minus(Number x, Number y) throws DirectiveExecutionException {
    NumberType outputType = getOutputType(x, y);
    if (outputType == null) {
      return null;
    }
    switch (outputType) {
      case SHORT:
        return (short) (x.shortValue() - y.shortValue());
      case INTEGER:
        return x.intValue() - y.intValue();
      case LONG:
        return x.longValue() - y.longValue();
      case FLOAT:
        return x.floatValue() - y.floatValue();
      case DOUBLE:
        return x.doubleValue() - y.doubleValue();
      default:
        BigDecimal bdX = numberToBigDecimal(x);
        BigDecimal bdY = numberToBigDecimal(y);
        return bdX.subtract(bdY).stripTrailingZeros();
    }
  }

  /**
   * Arithmetic operation - multiply any number of columns, of any types.
   * Output type is most general of input types.
   * Returns null if any input value is null.
   */
  public static Number multiply(Number... nums) throws DirectiveExecutionException {
    NumberType outputType = getOutputType(nums);
    if (outputType == null) {
      return null;
    }
    switch (outputType) {
      case SHORT:
        short shortProd = 1;
        for (Number num : nums) {
          shortProd *= num.shortValue();
        }
        return shortProd;
      case INTEGER:
        int intProd = 1;
        for (Number num : nums) {
          intProd *= num.intValue();
        }
        return intProd;
      case LONG:
        long longProd = 1;
        for (Number num : nums) {
          longProd *= num.longValue();
        }
        return longProd;
      case FLOAT:
        float floatProd = 1;
        for (Number num : nums) {
          floatProd *= num.floatValue();
        }
        return floatProd;
      case DOUBLE:
        double doubleProd = 1;
        for (Number num : nums) {
          doubleProd *= num.doubleValue();
        }
        return doubleProd;
      default:
        BigDecimal bdProd = BigDecimal.valueOf(1);
        for (Number num : nums) {
          bdProd = bdProd.multiply(numberToBigDecimal(num));
        }
        return bdProd;
    }
  }

  /**
   * Arithmetic operation - divides one column by another.
   * Output type is Double, except when there is an input of type BigDecimal, in which case the output is BigDecimal.
   * Returns null if any input value is null, or if the divisor equals 0.
   */
  public static Number divideq(Number x, Number y) throws DirectiveExecutionException {
    NumberType outputType = getOutputType(x, y);
    if (outputType == null || y.doubleValue() == 0) {
      return null;
    }
    switch (outputType) {
      case BIGDECIMAL:
        BigDecimal bdX = numberToBigDecimal(x);
        BigDecimal bdY = numberToBigDecimal(y);
        return bdX.divide(bdY, BigDecimal.ROUND_HALF_EVEN).stripTrailingZeros();
      default:
        return x.doubleValue() / y.doubleValue();
    }
  }

  /**
   * Arithmetic operation - divides one column by another and returns the remainder.
   * Output type is most general of input types, except 2 Shorts should give an Integer.
   * Returns null if any input value is null.
   */
  public static Number divider(Number x, Number y) throws DirectiveExecutionException {
    NumberType outputType = getOutputType(x, y);
    if (outputType == null || y.doubleValue() == 0) {
      return null;
    }
    switch (outputType) {
      case SHORT:
        return x.shortValue() % y.shortValue();
      case INTEGER:
        return x.intValue() % y.intValue();
      case LONG:
        return x.longValue() % y.longValue();
      case FLOAT:
        return x.floatValue() % y.floatValue();
      case DOUBLE:
        return x.doubleValue() % y.doubleValue();
      default:
        BigDecimal bdX = numberToBigDecimal(x);
        BigDecimal bdY = numberToBigDecimal(y);
        return bdX.remainder(bdY);
    }
  }

  /**
   * Arithmetic operation - calculates the LCM of two columns.
   * Output type is most general of input types.
   * Returns null if any input value is null.
   */
  public static Number lcm(Number x, Number y) throws DirectiveExecutionException {
    NumberType outputType = getOutputType(x, y);
    if (outputType == null) {
      return null;
    }
    BigDecimal bdX = numberToBigDecimal(x);
    BigDecimal bdY = numberToBigDecimal(y);
    switch (outputType) {
      case SHORT:
        return lcm(bdX, bdY).shortValue();
      case INTEGER:
        return lcm(bdX, bdY).intValue();
      case LONG:
        return lcm(bdX, bdY).longValue();
      case FLOAT:
        return lcm(bdX, bdY).floatValue();
      case DOUBLE:
        return lcm(bdX, bdY).doubleValue();
      default:
        BigDecimal pow = BigDecimal.valueOf(Math.pow(10, Math.max(bdX.scale(), bdY.scale())));
        BigInteger val1 = bdX.multiply(pow).toBigInteger();
        BigInteger val2 = bdY.multiply(pow).toBigInteger();
        BigDecimal absProduct = new BigDecimal(val1.multiply(val2).abs())
          .setScale(Math.min(bdX.scale(), bdY.scale()), BigDecimal.ROUND_HALF_EVEN);
        BigDecimal gcd = new BigDecimal(val1.gcd(val2));
        if (gcd.compareTo(BigDecimal.ZERO) == 0) {
          return BigDecimal.ZERO;
        }
        return absProduct.divide(gcd.multiply(pow), BigDecimal.ROUND_HALF_EVEN);
    }
  }

  /**
   Arithmetic operation - Check if all values are equal across any number of Short columns.
   */
  public static Boolean equal(Short... nums) {
    for (Short num : nums) {
      if (num == null) {
        return null;
      }
      if (!num.equals(nums[0])) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  /**
   Arithmetic operation - Check if all values are equal across any number of Integer columns.
   */
  public static Boolean equal(Integer... nums) {
    for (Integer num : nums) {
      if (num == null) {
        return null;
      }
      if (!num.equals(nums[0])) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  /**
   Arithmetic operation - Check if all values are equal across any number of Long columns.
   */
  public static Boolean equal(Long... nums) {
    for (Long num : nums) {
      if (num == null) {
        return null;
      }
      if (!num.equals(nums[0])) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  /**
   Arithmetic operation - Check if all values are equal across any number of Float columns.
   */
  public static Boolean equal(Float... nums) {
    for (Float num : nums) {
      if (num == null) {
        return null;
      }
      if (!num.equals(nums[0])) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  /**
   Arithmetic operation - Check if all values are equal across any number of Double columns.
   */
  public static Boolean equal(Double... nums) {
    for (Double num : nums) {
      if (num == null) {
        return null;
      }
      if (!num.equals(nums[0])) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  /**
   Arithmetic operation - Check if all values are equal across any number of BigDecimal columns.
   */
  public static Boolean equal(BigDecimal... nums) {
    for (BigDecimal num : nums) {
      if (num == null) {
        return null;
      }
      if (!num.equals(nums[0])) {
        return Boolean.FALSE;
      }
    }
    return Boolean.TRUE;
  }

  /**
   * Arithmetic operation - Find the maximum of any number of columns, of any valid types.
   * Output type is most general of input types.
   * Returns null if any input value is null.
   */
  public static Number max(Number... nums) throws DirectiveExecutionException {
    NumberType outputType = getOutputType(nums);
    if (outputType == null) {
      return null;
    }
    switch (outputType) {
      case SHORT:
        short shortMax = nums[0].shortValue();
        for (Number num : nums) {
          if (num.shortValue() > shortMax) {
            shortMax = num.shortValue();
          }
        }
        return shortMax;
      case INTEGER:
        int intMax = nums[0].intValue();
        for (Number num : nums) {
          if (num.intValue() > intMax) {
            intMax = num.intValue();
          }
        }
        return intMax;
      case LONG:
        long longMax = nums[0].longValue();
        for (Number num : nums) {
          if (num.longValue() > longMax) {
            longMax = num.longValue();
          }
        }
        return longMax;
      case FLOAT:
        float floatMax = nums[0].floatValue();
        for (Number num : nums) {
          if (num.floatValue() > floatMax) {
            floatMax = num.floatValue();
          }
        }
        return floatMax;
      case DOUBLE:
        double doubleMax = nums[0].doubleValue();
        for (Number num : nums) {
          if (num.doubleValue() > doubleMax) {
            doubleMax = num.doubleValue();
          }
        }
        return doubleMax;
      default:
        BigDecimal bdMax = new BigDecimal(nums[0].toString());
        for (Number num : nums) {
          bdMax = bdMax.max(numberToBigDecimal(num));
        }
        return bdMax;
    }
  }

  /**
   * Arithmetic operation - Find the minimum of any number of columns, of any valid types.
   * Output type is most general of input types.
   * Returns null if any input value is null.
   */
  public static Number min(Number... nums) throws DirectiveExecutionException {
    NumberType outputType = getOutputType(nums);
    if (outputType == null) {
      return null;
    }
    switch (outputType) {
      case SHORT:
        short shortMin = nums[0].shortValue();
        for (Number num : nums) {
          if (num.shortValue() < shortMin) {
            shortMin = num.shortValue();
          }
        }
        return shortMin;
      case INTEGER:
        int intMin = nums[0].intValue();
        for (Number num : nums) {
          if (num.intValue() < intMin) {
            intMin = num.intValue();
          }
        }
        return intMin;
      case LONG:
        long longMin = nums[0].longValue();
        for (Number num : nums) {
          if (num.longValue() < longMin) {
            longMin = num.longValue();
          }
        }
        return longMin;
      case FLOAT:
        float floatMin = nums[0].floatValue();
        for (Number num : nums) {
          if (num.floatValue() < floatMin) {
            floatMin = num.floatValue();
          }
        }
        return floatMin;
      case DOUBLE:
        double doubleMin = nums[0].doubleValue();
        for (Number num : nums) {
          if (num.doubleValue() < doubleMin) {
            doubleMin = num.doubleValue();
          }
        }
        return doubleMin;
      default:
        BigDecimal bdMin = new BigDecimal(nums[0].toString());
        for (Number num : nums) {
          bdMin = bdMin.min(numberToBigDecimal(num));
        }
        return bdMin;
    }
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
        BigDecimal numer = numberToBigDecimal(num).subtract((BigDecimal) avg);
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
