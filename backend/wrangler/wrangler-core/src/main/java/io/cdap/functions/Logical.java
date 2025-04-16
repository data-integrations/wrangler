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
 * The collection of logical functions that operate on bit.
 */
public final class Logical {

  /**
   * Don't let anyone instantiate this class.
   */
  private Logical() {}

  /**
   * Bitwise 'AND' operation of two numbers. The inputs must be non-null.
   *
   * @param num1 first number.
   * @param num2 second number.
   * @return a result of bitwise 'AND'.
   */
  public static long BitAnd(long num1, long num2) {
    return num1 & num2;
  }

  /**
   * Bitwise 'AND' operation of two numbers. The inputs must be non-null.
   *
   * @param num1 first number.
   * @param num2 second number.
   * @return a result of bitwise 'AND'.
   */
  public static int BitAnd(int num1, int num2) {
    return num1 & num2;
  }

  /**
   * Bitwise 'OR' operation of two numbers. The inputs must be non-null.
   *
   * @param num1 first number.
   * @param num2 second number.
   * @return a result of bitwise 'OR'
   */
  public static long BitOr(long num1, long num2) {
    return num1 | num2;
  }

  /**
   * Bitwise 'OR' operation of two numbers. The inputs must be non-null.
   *
   * @param num1 first number.
   * @param num2 second number.
   * @return a result of bitwise 'OR'
   */
  public static int BitOr(int num1, int num2) {
    return num1 | num2;
  }

  /**
   * Bitwise 'XOR' operation of two numbers. The inputs must be non-null.
   *
   * @param num1 first number.
   * @param num2 second number.
   * @return a result of bitwise 'XOR'
   */
  public static long BitXor(long num1, long num2) {
    return num1 ^ num2;
  }

  /**
   * Bitwise 'XOR' operation of two numbers. The inputs must be non-null.
   *
   * @param num1 first number.
   * @param num2 second number.
   * @return a result of bitwise 'XOR'
   */
  public static int BitXor(int num1, int num2) {
    return num1 ^ num2;
  }

  /**
   * Returns the complement of the logical value of an expression.
   * A numeric expression that evaluates to 0 is a logical value of false.
   * A numeric expression that evaluates to anything else is a logical true.
   * If the value of expression is true, the Not function returns a value of false (0).
   * If the value of expression is false, the Not function returns a value of true (1).
   *
   * @param val expression result.
   * @return 1 or 0.
   */
  public static int Not(double val) {
    return val == 0d ? 1 : 0;
  }

  /**
   * Returns the complement of the logical value of the given string.
   * An empty string is logically false. All other string expressions, including strings that include
   * an empty string, spaces, or the number 0 and spaces, are logically true.
   * If the logical value of the string is true, the Not function returns a value of false (0).
   * If the logical value of the string is false, the Not function returns a value of true (1).
   *
   * @param val string value.
   * @return 1 or 0.
   */
  public static int Not(String val) {
    return (val == null || val.isEmpty()) ? 1 : 0;
  }

  /**
   * Returns the complement of the logical value of an expression.
   * A numeric expression that evaluates to 0 is a logical value of false.
   * A numeric expression that evaluates to anything else is a logical true.
   * If the value of expression is true, the Not function returns a value of false (0).
   * If the value of expression is false, the Not function returns a value of true (1).
   *
   * @param val string value.
   * @return 1 or 0.
   */
  public static int Not(float val) {
    return val == 0f ? 1 : 0;
  }

  /**
   * Returns the complement of the logical value of an expression.
   * A numeric expression that evaluates to 0 is a logical value of false.
   * A numeric expression that evaluates to anything else is a logical true.
   * If the value of expression is true, the Not function returns a value of false (0).
   * If the value of expression is false, the Not function returns a value of true (1).
   *
   * @param val string value.
   * @return 1 or 0.
   */
  public static int Not(int val) {
    return val == 0 ? 1 : 0;
  }

  /**
   * Returns the complement of the logical value of an expression.
   * A numeric expression that evaluates to 0 is a logical value of false.
   * A numeric expression that evaluates to anything else is a logical true.
   * If the value of expression is true, the Not function returns a value of false (0).
   * If the value of expression is false, the Not function returns a value of true (1).
   *
   * @param val string value.
   * @return 1 or 0.
   */
  public static int Not(long val) {
    return val == 0L ? 1 : 0;
  }

  /**
   * Returns the long made from the string argument, which contains a binary representation of "1"s and "0"s.
   *
   * @param value to be compressed.
   * @return long value of compressed binary string.
   */
  public static long BitCompress(String value) {
    return Long.parseUnsignedLong(value, 2);
  }

  /**
   * Returns a string containing the binary representation in "1"s and "0"s of the given long.
   *
   * @param value to be expanded into '1' and '0' binary representation.
   * @return a binary string representation of value.
   */
  public static String BitExpand(long value) {
    return Long.toBinaryString(value);
  }

  /**
   * Returns a long with specific bits set to a specific state.
   *
   * @param value to perform action on.
   * @param positions array of positions.
   * @param bit to be set. Either 1 or 0.
   * @return a long represents the bit set based on positions specified.
   */
  public static long SetBit(long value, int[] positions, int bit) {
    long result = value;
    for (int position : positions) {
      position = position - 1;
      if (bit > 0) {
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
