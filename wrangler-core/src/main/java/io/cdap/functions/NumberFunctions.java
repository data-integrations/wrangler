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

import java.math.BigDecimal;
import javax.annotation.Nullable;

public final class NumberFunctions {

  /**
   * @return The number as double.
   */
  @Nullable
  public static Double AsDouble(@Nullable Number value) {
    return value == null ? null : value.doubleValue();
  }

  /**
   * @return The number as float.
   */
  @Nullable
  public static Float AsFloat(@Nullable Number value) {
    return value == null ? null : value.floatValue();
  }

  /**
   * @return The number as integer.
   */
  @Nullable
  public static Integer AsInteger(@Nullable Number value) {
    return value == null ? null : value.intValue();
  }

  /**
   * @return Returns the mantissa from the given number. Mantissa definition is used from
   * https://mathworld.wolfram.com/Mantissa.html
   */
  public static double Mantissa(int value) {
    return 0d;
  }

  /**
   * @return Returns the mantissa from the given number. Mantissa definition is used from
   * https://mathworld.wolfram.com/Mantissa.html
   */
  public static double Mantissa(long value) {
    return 0d;
  }

  /**
   * @return Returns the mantissa from the given number. Mantissa definition is used from
   * https://mathworld.wolfram.com/Mantissa.html
   */
  public static double Mantissa(float value) {
    return Mantissa(new BigDecimal(String.valueOf(value)));
  }

  /**
   * @return Returns the mantissa from the given number. Mantissa definition is used from
   * https://mathworld.wolfram.com/Mantissa.html
   */
  public static double Mantissa(double value) {
    return Mantissa(new BigDecimal(String.valueOf(value)));
  }

  /**
   * @return Returns the mantissa from the given number. Mantissa definition is used from
   * https://mathworld.wolfram.com/Mantissa.html. If value is null, it will return 0.
   */
  public static double Mantissa(BigDecimal value) {
    return value == null ? 0d : value.subtract(new BigDecimal(value.intValue())).doubleValue();
  }
}
