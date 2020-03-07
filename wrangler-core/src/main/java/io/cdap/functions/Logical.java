/*
 *  Copyright © 2020 Cask Data, Inc.
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

/**
 * The collection of logical bitwise functions.
 */
public final class Logical {

  /**
   * Bitwise 'and' operation of two numbers.
   *
   * @param num1 first number
   * @param num2 second number
   * @return a result of bitwise 'and'.
   */
  public static long BitAnd(long num1, long num2) {
    return num1 & num2;
  }

  public static long BitOr(long num1, long num2) {
    return num1 | num2;
  }

  public static long BitXor(long num1, long num2) {
    return num1 ^ num2;
  }

  public static short Not(double num) {
    if (num < 0) {
      return 0;
    }
    return 1;
  }

  public static short Not(String val) {
    if (val == null) {
      return 0;
    }
    return 1;
  }

  public static long BitCompress(String value) {
    long result = Long.parseUnsignedLong(value, 2);
    return result;
  }

  public static String BitExpand(long value) {
    return Long.toBinaryString(value);
  }

  public static long SetBit(long value, int[] positions, int bit) {
    if (bit > 0) {
      bit = 1;
    } else {
      bit = 0;
    }

    long result = value;
    for (int position : positions) {
      position = position - 1;
      if (bit == 1) {
        result = result | (1 << position);
        result |= 1 << position;
      } else {
        result = result & ~(1 << position);
        result &= ~(1 << position);
      }
    }
    return result;
  }
}
