/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.functions;

import co.cask.wrangler.api.Row;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;

/**
 * Data Quality Checks consolidated.
 */
public class DataQuality extends Types {

  /**
   * Given a row, finds the length of the row.
   *
   * @param row length needs to be determined.
   * @return length of the row.
   */
  public static int columns(Row row) {
    return row.length();
  }

  /**
   * Finds if the row has a column.
   *
   * @param row in which a column needs to be checked.
   * @param column name of the column to be checked.
   * @return true if column is not null and exists, false otherwise.
   */
  public static boolean hascolumn(Row row, String column) {
    if (column == null) {
      return false;
    }
    return row.find(column) != -1 ? true : false;
  }

  /**
   * Checks if the value is within the range.
   *
   * @param value to be checked if it's in the range.
   * @param lower end of the defined range.
   * @param upper end of the defined range inclusive.
   * @return true if in range, false otherwise.
   */
  public static boolean inrange(double value, double lower, double upper) {
    Range<Double> range = Range.range(lower, BoundType.CLOSED, upper, BoundType.CLOSED);
    if (range.contains(value)) {
      return true;
    }
    return false;
  }

  /**
   * Returns the length of the string.
   *
   * @param str for which we need to determine the length.
   * @return length of string if not null, 0 otherwise.
   */
  public static int strlen(String str) {
    if (str != null) {
      return str.length();
    }
    return 0;
  }

  /**
   * Checks if the object is null.
   *
   * @param object to be checked for null.
   * @return true if
   */
  public static boolean isnull(Object object) {
    return object == null ? true : false;
  }

  /**
   * Checks if the string is empty or not.
   *
   * @param str to be checked for empty.
   * @return true if not null and empty, else false.
   */
  public static boolean isempty(String str) {
    if (str != null && str.isEmpty()) {
      return true;
    }
    return false;
  }

}
