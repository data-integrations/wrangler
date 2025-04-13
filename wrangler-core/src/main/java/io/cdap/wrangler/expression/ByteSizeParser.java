/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.wrangler.expression;

public class ByteSizeParser {
  public static long parse(String input) {
    input = input.trim().toUpperCase();

    if (input.endsWith("KB")) {
      return (long) (Double.parseDouble(input.replace("KB", "")) * 1024);
    } else if (input.endsWith("MB")) {
      return (long) (Double.parseDouble(input.replace("MB", "")) * 1024 * 1024);
    } else if (input.endsWith("GB")) {
      return (long) (Double.parseDouble(input.replace("GB", "")) * 1024 * 1024 * 1024);
    } else if (input.endsWith("B")) {
      return (long) Double.parseDouble(input.replace("B", ""));
    } else {
      throw new IllegalArgumentException("Invalid byte size: " + input);
    }
  }
}
