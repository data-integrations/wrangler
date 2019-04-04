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

import io.cdap.wrangler.dq.TypeInference;

/**
 * Static class that is included in the Jexl expression for detecting types of data.
 */
public class Types {
  /**
   * Checks if a value is a date or not.
   *
   * @param value representing date.
   * @return true if date, else false.
   */
  public static boolean isDate(String value) {
    return TypeInference.isDate(value);
  }

  /**
   * Checks if a value is a datetime or not.
   *
   * @param value representing date time.
   * @return true if datetime, else false.
   */
  public static boolean isTime(String value) {
    return TypeInference.isTime(value);
  }

  /**
   * Checks if a value is a number or not.
   *
   * @param value representing a number.
   * @return true if number, else false.
   */
  public static boolean isNumber(String value) {
    return TypeInference.isNumber(value);
  }


  /**
   * Checks if a value is a boolean or not.
   *
   * @param value representing a boolean.
   * @return true if boolean, else false.
   */
  public static boolean isBoolean(String value) {
    return TypeInference.isBoolean(value);
  }

  /**
   * Checks if a value is a empty or not.
   *
   * @param value representing a empty.
   * @return true if empty, else false.
   */
  public static boolean isEmpty(String value) {
    return TypeInference.isEmpty(value);
  }

  /**
   * Checks if a value is a double or not.
   *
   * @param value representing a double.
   * @return true if double, else false.
   */
  public static boolean isDouble(String value) {
    return TypeInference.isDouble(value);
  }

  /**
   * Checks if a value is a integer or not.
   *
   * @param value representing a integer.
   * @return true if integer, else false.
   */
  public static boolean isInteger(String value) {
    return TypeInference.isInteger(value);
  }
}

