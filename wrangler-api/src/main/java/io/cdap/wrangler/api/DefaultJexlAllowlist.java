/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.wrangler.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Defines the set of classes allowed in the JEXL Sandbox by default.
 */
public final class DefaultJexlAllowlist {

  private static final List<String> DEFAULT_CLASSES = Arrays.asList(
      // Data types
      "java.lang.Boolean", "java.lang.Byte", "java.lang.Character", "java.lang.Double", "java.lang.Float",
      "java.lang.Integer", "java.lang.Long", "java.lang.Short",

      // Strings
      "java.lang.String", "java.lang.StringBuilder", "java.util.StringJoiner",

      // Math
      "java.lang.Math", "java.math.BigDecimal", "java.math.BigInteger",

      // Time
      "java.time.ZonedDateTime", "java.time.LocalDate", "java.time.LocalDateTime", "java.time.Instant",
      "java.time.Duration", "java.time.format.DateTimeFormatter",

      // Utilities
      "java.util.Arrays", "java.util.Collections", "java.util.UUID", "java.util.Base64");

  private static final List<JexlAllowlist> ALLOWLIST = Collections.unmodifiableList(
      DEFAULT_CLASSES.stream()
          .map(className -> {
            return new JexlAllowlist(
                Objects.requireNonNull(className),
                Collections.singletonList(JexlAllowlist.INCLUDE_ALL_WILDCARD),
                Collections.singletonList(JexlAllowlist.INCLUDE_ALL_WILDCARD));
          })
          .collect(Collectors.toList()));

  private DefaultJexlAllowlist() {
  }

  /**
   * @return the list of default allowed classes.
   */
  public static List<JexlAllowlist> get() {
    return ALLOWLIST;
  }
}
