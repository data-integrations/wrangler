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

package io.cdap.wrangler.dq;

import java.math.BigInteger;
import java.util.regex.Pattern;

/**
 * Type Interface provides utility functions that allow you to detect the types of data.
 */
public class TypeInference {
  private static final Pattern patternInteger = Pattern.compile("^(\\+|-)?\\d+$");

  private static final Pattern patternDouble = Pattern.compile(
    "^[-+]?"// Positive/Negative sign
      + "("// BEGIN Decimal part
      + "[0-9]+([,\\.][0-9]+)?|"// Alternative I (w/o grouped integer part)
      + "(" // BEGIN Alternative II (with grouped integer part)
      + "[0-9]{1,3}" // starting digits
      + "(" // BEGIN grouped part
      + "((,[0-9]{3})*"// US integer part
      + "(\\.[0-9]+)?"// US float part
      + "|" // OR
      + "((\\.[0-9]{3})*|([ \u00A0\u2007\u202F][0-9]{3})*)"// EU integer part
      + "(,[0-9]+)?)"// EU float part
      + ")"// END grouped part
      + ")" // END Alternative II
      + ")" // END Decimal part
      + "([ ]?[eE][-+]?[0-9]+)?$"); // scientific part

  /**
   * Detect if the given value is a double type.
   *
   * <p>
   * Note:<br>
   * 1. This method support only English locale.<br>
   * e.g. {@code TypeInference.isDouble("3.4")} returns {@code true}.<br>
   * e.g. {@code TypeInference.isDouble("3,4")} returns {@code false}.<br>
   * 2. Exponential notation can be detected as a valid double.<br>
   * e.g. {@code TypeInference.isDouble("1.0E+4")} returns {@code true}.<br>
   * e.g. {@code TypeInference.isDouble("1.0e-4")} returns {@code true}.<br>
   * e.g. {@code TypeInference.isDouble("1.0e-04")} returns {@code true}.<br>
   * 3. Numbers marked with a type is invalid.<br>
   * e.g. {@code TypeInference.isDouble("3.4d")} returns {@code false}.<br>
   * e.g. {@code TypeInference.isDouble("123L")} returns {@code false}.<br>
   * 4. White space is invalid.<br>
   * e.g. {@code TypeInference.isDouble(" 3.4")} returns {@code false}.<br>
   * e.g. {@code TypeInference.isDouble("3.4 ")} returns {@code false}.<br>
   * 5. "." is not obligatory.<br>
   * e.g. {@code TypeInference.isDouble("100")} returns {@code true}.
   * <P>
   *
   * @param value the value to be detected.
   * @return true if the value is a double type, false otherwise.
   */
  public static boolean isDouble(String value) {
    if (!isEmpty(value) && patternDouble.matcher(value).matches()) {
      return true;
    }
    return false;
  }

  /**
   * Detect if the given value is a integer type.
   *
   * @param value the value to be detected.
   * @return true if the value is a integer type, false otherwise.
   */
  public static boolean isInteger(String value) {
    if (!isEmpty(value) && patternInteger.matcher(value).matches()) {
      return true;
    }
    return false;
  }

  public static boolean isNumber(String value) {
    return isDouble(value) || isInteger(value);

  }

  /**
   * Get big integer from a string.
   *
   * @param value
   * @return big integer instance , or null if numer format exception occurrs.
   */
  public static BigInteger getBigInteger(String value) {
    BigInteger bint = null;
    try {
      bint = new BigInteger(value);
    } catch (NumberFormatException e) {
      return null;
    }
    return bint;
  }

  /**
   * Detect if the given value is a boolean type.
   *
   * @param value the value to be detected.
   * @return true if the value is a boolean type, false otherwise.
   */
  public static boolean isBoolean(String value) {
    if (isEmpty(value)) {
      return false;
    }
    if ((value.trim().length() == 4 || value.trim().length() == 5)
      && ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value))) { //$NON-NLS-1$ //$NON-NLS-2$
      return true;
    }
    return false;
  }

  /**
   * Detect if the given value is a date type. <br>
   *
   * @param value the value to be detected.
   * @return true if the value is a date type, false otherwise.
   */
  public static boolean isDate(String value) {
    return DateTimePattern.isDate(value);
  }

  /**
   * Detect if the given value is a time type.
   *
   * @param value
   * @return
   */
  public static boolean isTime(String value) {
    return DateTimePattern.isTime(value);
  }

  /**
   * Detect if the given value is blank or null.
   *
   * @param value the value to be detected.
   * @return true if the value is blank or null, false otherwise.
   */
  public static boolean isEmpty(String value) {
    return value == null || value.trim().length() == 0;
  }

  /**
   *
   * @param type the expected type
   * @param value the value to be detected
   * @return true if the type of value is expected, false otherwise.
   */
  public static boolean isValid(DataType type, String value) {
    switch (type) {
      case BOOLEAN:
        return isBoolean(value);
      case INTEGER:
        return isInteger(value);
      case DOUBLE:
        return isDouble(value);
      case DATE:
        return isDate(value);
      case STRING:
        // Everything can be a string
        return true;
      default:
        // Unsupported type
        return false;
    }
  }

  public static DataType getDataType(String value) {
    if (TypeInference.isEmpty(value)) {
      // 1. detect empty
      return DataType.EMPTY;
    } else if (TypeInference.isBoolean(value)) {
      // 2. detect boolean
      return DataType.BOOLEAN;
    } else if (TypeInference.isInteger(value)) {
      // 3. detect integer
      return DataType.INTEGER;
    } else if (TypeInference.isDouble(value)) {
      // 4. detect double
      return DataType.DOUBLE;
    } else if (isDate(value)) {
      // 5. detect date
      return DataType.DATE;
    } else if (isTime(value)) {
      // 6. detect date
      return DataType.TIME;
    }
    // will return string when no matching
    return DataType.STRING;
  }

}
