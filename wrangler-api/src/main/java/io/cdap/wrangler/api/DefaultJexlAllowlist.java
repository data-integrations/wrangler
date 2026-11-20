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
      "java.time.ZonedDateTime", "java.time.LocalDate", "java.time.LocalTime", "java.time.LocalDateTime",
      "java.time.Instant", "java.time.Duration", "java.time.Period", "java.time.ZoneId",
      "java.time.ZoneOffset", "java.time.ZoneRegion", "java.time.DayOfWeek", "java.time.Month",
      "java.time.Year", "java.time.format.DateTimeFormatter", "java.util.Date",

      // Collections and Utilities
      "java.util.Arrays", "java.util.Arrays$ArrayList", "java.util.Collections",
      "java.util.Collections$EmptyList", "java.util.Collections$EmptyMap", "java.util.Collections$EmptySet",
      "java.util.Collections$SingletonList", "java.util.Collections$SingletonMap",
      "java.util.Collections$SingletonSet", "java.util.Objects", "java.util.UUID",
      "java.util.Base64", "java.util.Base64$Decoder", "java.util.Base64$Encoder",
      "java.util.List", "java.util.ArrayList", "java.util.LinkedList",
      "java.util.Set", "java.util.HashSet", "java.util.LinkedHashSet", "java.util.TreeSet",
      "java.util.Map", "java.util.HashMap", "java.util.LinkedHashMap", "java.util.TreeMap",
      "java.util.regex.Pattern", "java.util.regex.Matcher",

      // JSON and CDAP / Wrangler row and schema types
      "com.google.gson.JsonElement", "com.google.gson.JsonObject", "com.google.gson.JsonArray",
      "com.google.gson.JsonPrimitive", "com.google.gson.JsonNull",
      "io.cdap.cdap.api.common.Bytes", "io.cdap.cdap.api.data.format.StructuredRecord",
      "io.cdap.cdap.api.data.schema.Schema", "io.cdap.cdap.api.data.schema.Schema$Field",
      "io.cdap.cdap.api.data.schema.Schema$Type", "io.cdap.wrangler.api.Pair",
      "io.cdap.wrangler.api.Row", "io.cdap.wrangler.expression.ELContext$Context",

      // Commonly used default registered functions in EL
      "io.cdap.functions.Global", "io.cdap.functions.DateAndTime", "io.cdap.functions.Dates",
      "io.cdap.functions.JsonFunctions", "io.cdap.wrangler.utils.DecimalTransform",
      "io.cdap.wrangler.utils.ArithmeticOperations", "org.apache.commons.lang.StringUtils",
      "com.google.common.base.Strings", "org.apache.commons.lang3.StringEscapeUtils",
      "io.cdap.functions.DataQuality", "io.cdap.functions.Types", "io.cdap.functions.DDL",
      "io.cdap.functions.GeoFences", "io.cdap.functions.Logical", "io.cdap.functions.NumberFunctions");

  private static final List<JexlAllowlist> ALLOWLIST = Collections.unmodifiableList(
      DEFAULT_CLASSES.stream()
          .map(className -> {
            return new JexlAllowlist(
                className,
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
