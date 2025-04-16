/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.wrangler.utils;

import java.util.regex.Pattern;

/**
 * Utility methods for reference name
 */
public class ReferenceNames {
  private static final Pattern DATASET_PATTERN = Pattern.compile("[$\\.a-zA-Z0-9_-]+");
  private static final String REGEX = "[^$\\.a-zA-Z0-9_-]+";

  private ReferenceNames() {
  }

  /**
   * Check if a given reference name is valid
   *
   * @param referenceName reference name to check
   */
  public static void validate(String referenceName) {
    if (!DATASET_PATTERN.matcher(referenceName).matches()) {
      throw new IllegalArgumentException(
        String.format("Invalid reference name '%s'. Supported characters are: letters, " +
                        "numbers, and '_', '-', '.', or '$'.", referenceName));
    }
  }

  /**
   * Cleanse the given reference name. This method will remove all the disallowed characters in the given reference
   * name. For example, 111-22-33(1).csv will get convert to 111-22-331.csv. If no valid characters, the method will
   * return "sample".
   *
   * @param referenceName the old reference name
   * @return the reference name with only allowed characters
   */
  public static String cleanseReferenceName(String referenceName) {
    String result = referenceName.replaceAll(REGEX, "");
    return result.isEmpty() ? "sample" : result;
  }
}
