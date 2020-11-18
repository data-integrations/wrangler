/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.functions;

import com.google.common.base.Strings;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column <expression>
 */
public final class Global {
  private Global() {
  }

  /**
   * Converts String value to double.
   *
   * @param value of type String to be converted to double.
   * @return double value of the string passed.
   */
  public static double toDouble(String value) {
    return Double.parseDouble(value);
  }

  /**
   * Coverts a String value to float.
   *
   * @param value of type string to be converted to float.
   * @return float value of the string passed.
   */
  public static float toFloat(String value) {
    return Float.parseFloat(value);
  }

  /**
   * Converts a String value to Long.
   *
   * @param value  of type string to be converted to float.
   * @return  float value of the string passed.
   */
  public static long toLong(String value) {
    return Long.parseLong(value);
  }

  /**
   * Converts a String value to integer.
   *
   * @param value  of type string to be converted to integer.
   * @return  integer value of the string passed.
   */
  public static int toInteger(String value) {
    return Integer.parseInt(value);
  }

  /**
   * Converts a String value to byte array.
   *
   * @param value  of type string to be converted to byte array.
   * @return  byte array value of the string passed.
   */
  public static byte[] toBytes(String value) {
    return value.getBytes();
  }

  /**
   * Concats two string without any separator in between.
   *
   * @param a First string
   * @param b Second String
   * @return concated Strings
   */
  public static String concat(String a, String b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return a.concat(b);
  }

  /**
   * Concats two string with a delimiter.
   *
   * @param a first string.
   * @param delim delimiter.
   * @param b second string.
   * @return concated string.
   */
  public static String concat(String a, String delim, String b) {
    if (a == null && b != null) {
      return delim.concat(b);
    } else if (b == null && a != null) {
      return a.concat(delim);
    }
    return a.concat(delim).concat(b);
  }

  /**
   * Finds the first non-null object.
   *
   * @param objects to be check for null.
   * @return first non-null object.
   */
  public static Object coalesce(Object ... objects) {
    for (Object object : objects) {
      if (object != null) {
        return object;
      }
    }
    return objects.length > 0 ? objects[0] : null;
  }

  /**
   * Finds the last non-null object.
   *
   * @param objects to be check for null.
   * @return first non-null object.
   */
  public static Object rcoalesce(Object ... objects) {
    int idx = objects.length - 1;
    while (idx >= 0) {
      if (objects[idx] != null) {
        return objects[idx];
      }
      idx = idx - 1;
    }
    return objects.length > 0 ? objects[objects.length - 1] : null;
  }

  /**
   * Formats the string in way similar to String format.
   *
   * @param str format of string.
   * @param args arguments to included in the string.
   * @return A formatted string.
   */
  public static String format(String str, Object... args) {
    return String.format(str, args);
  }

  /**
   * Returns a string, of length at least {@code minLength}, consisting of {@code string} prepended
   * with as many copies of {@code padChar} as are necessary to reach that length. For example,
   *
   * <ul>
   * <li>{@code padStart("7", 3, '0')} returns {@code "007"}
   * <li>{@code padStart("2010", 3, '0')} returns {@code "2010"}
   * </ul>
   * @return the padded string.
   */
  public static String padAtStart(String string, int minLength, char padChar) {
    return Strings.padStart(string, minLength, padChar);
  }

  /**
   * Returns a string, of length at least {@code minLength}, consisting of {@code string} appended
   * with as many copies of {@code padChar} as are necessary to reach that length. For example,
   *
   * <ul>
   * <li>{@code padEnd("4.", 5, '0')} returns {@code "4.000"}
   * <li>{@code padEnd("2010", 3, '!')} returns {@code "2010"}
   * </ul>
   *
   * @return the padded string
   */
  public static String padAtEnd(String string, int minLength, char padChar) {
    return Strings.padEnd(string, minLength, padChar);
  }

  /**
   * Returns a string consisting of a specific number of concatenated copies of an input string. For
   * example, {@code repeat("hey", 3)} returns the string {@code "heyheyhey"}.
   *
   * @param string any non-null string
   * @param count the number of times to repeat it; a nonnegative integer
   * @return a string containing {@code string} repeated {@code count} times (the empty string if
   *     {@code count} is zero)
   * @throws IllegalArgumentException if {@code count} is negative
   */
  public static String repeat(String string, int count) {
    return Strings.repeat(string, count);
  }

  /**
   * This String util method removes single or double quotes
   * from a string if its quoted.
   * for input string = "mystr1" output will be = mystr1
   * for input string = 'mystr2' output will be = mystr2
   *
   * @param string value to be unquoted.
   * @return value unquoted, null if input is null.
   *
   */
  public static String unquote(String string) {

    if (string != null && ((string.startsWith("\"") && string.endsWith("\""))
      || (string.startsWith("'") && string.endsWith("'")))) {
      string = string.substring(1, string.length() - 1);
    }
    return string;
  }

  /**
   * Returns true when an expression does not evaluate to the null value.
   *
   * @param value to be evaluated.
   * @return true when not null, false otherwise.
   */
  public static boolean IsNotNull(Object value) {
    return value != null;
  }

  /**
   * Returns true when an expression evaluates to the null value.
   *
   * @param value to be evaluated.
   * @return false when not null, true otherwise.
   */
  public static boolean IsNull(Object value) {
    return !IsNotNull(value);
  }

  /**
   * Returns an empty string if the input column is null, otherwise returns the input column value.
   *
   * @param value to be evaluated.
   * @return Empty string if null, else 'value'
   */
  public static Object NullToEmpty(Object value) {
    if (IsNull(value)) {
      return "";
    }
    return value;
  }

  /**
   * Returns zero if the input column is null, otherwise returns the input column value.
   *
   * @param value to be evaluated.
   * @return Empty string if null, else 'value'
   */
  public static Object NullToZero(Object value) {
    if (IsNull(value)) {
      return 0;
    }
    return value;
  }

  /**
   * Returns the specified value if the input column is null, otherwise returns the input column value.
   *
   * @param value to evaluated.
   * @param replace value to replace with.
   * @return value if not null, else with replaced value.
   */
  public static Object NullToValue(Object value, Object replace) {
    if (IsNull(value)) {
      return replace;
    }
    return value;
  }
}
