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

package io.cdap.wrangler.api.parser;

/**
 * Utility methods for string manipulation.
 */
public final class StringUtils {
  /** Private constructor to prevent instantiation. */
  private StringUtils() {
    // Utility class should not be instantiated
  }

  /**
   * Checks if a string is quoted.
   *
   * @param str The string to check
   * @return True if string starts and ends with quotes
   */
  public static boolean isQuoted(final String str) {
    return str.startsWith("\"") && str.endsWith("\"");
  }

  /**
   * Removes quotes from a string.
   *
   * @param str The string to unquote
   * @return String with quotes removed if present
   */  public static String trim(final String str) {
    if (isQuoted(str)) {
      return str.substring(1, str.length() - 1);
    }    return str;
  }
}