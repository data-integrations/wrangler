/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation.functions;

import java.io.Serializable;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column <expression>
 */
public final class Conversions implements Serializable {
  /**
   * Converts String value to double.
   *
   * @param value of type String to be converted to double.
   * @return double value of the string passed.
   */
  public static double DOUBLE(String value) {
    return Double.parseDouble(value);
  }

  /**
   * Coverts a String value to float.
   *
   * @param value of type string to be converted to float.
   * @return float value of the string passed.
   */
  public static float FLOAT(String value) {
    return Float.parseFloat(value);
  }

  /**
   * Converts a String value to Long.
   *
   * @param value  of type string to be converted to float.
   * @return  float value of the string passed.
   */
  public static long LONG(String value) {
    return Long.parseLong(value);
  }

  /**
   * Converts a String value to integer.
   *
   * @param value  of type string to be converted to integer.
   * @return  integer value of the string passed.
   */
  public static int INT(String value) {
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

}
