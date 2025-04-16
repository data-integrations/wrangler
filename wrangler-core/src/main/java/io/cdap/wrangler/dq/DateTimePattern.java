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

package io.cdap.wrangler.dq;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Date time patterns
 */
public class DateTimePattern {
  private static final List<Map<Pattern, String>> DATE_PATTERN_GROUP_LIST = new ArrayList<>();
  private static final List<Map<Pattern, String>> TIME_PATTERN_GROUP_LIST = new ArrayList<>();

  static {
    loadPatterns("DateRegexesGrouped.txt", DATE_PATTERN_GROUP_LIST);
    // Load time patterns
    loadPatterns("TimeRegexes.txt", TIME_PATTERN_GROUP_LIST);
  }

  private static void loadPatterns(String patternFileName, List<Map<Pattern, String>> patternParsers) {
    InputStream stream = DateTimePattern.class.getClassLoader().getResourceAsStream(patternFileName);
    try {
      List<String> lines = IOUtils.readLines(stream, "UTF-8");
      Map<Pattern, String> currentGroupMap = new LinkedHashMap<>();
      patternParsers.add(currentGroupMap);
      for (String line : lines) {
        if (!"".equals(line.trim())) { // Not empty
          if (line.startsWith("--")) { // group separator
            currentGroupMap = new LinkedHashMap<>();
            patternParsers.add(currentGroupMap);
          } else {
            String[] lineArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, "\t");
            String format = lineArray[0];
            Pattern pattern = Pattern.compile(lineArray[1]);
            currentGroupMap.put(pattern, format);
          }
        }
      }
      stream.close();
    } catch (IOException e) {
      // Throw exception
    }
  }

  /**
   * Whether the given string value is a date or not.
   *
   * @param value
   * @return true if the value is a date.
   */
  public static boolean isDate(String value) {
    if (StringUtils.isEmpty(value)) {
      return false;
    }
    // The length of date strings must not be less than 6, and must not exceed 64.
    if (value.length() < 6 || value.length() > 64) {
      return false;
    }
    return isDateTime(DATE_PATTERN_GROUP_LIST, value);
  }

  /**
   * Check if the value passed is a time or not.
   *
   * @param value
   * @return true if the value is type "Time", false otherwise.
   */
  public static boolean isTime(String value) {
    if (StringUtils.isEmpty(value)) {
      return false;
    }
    // The length of date strings must not be less than 4, and must not exceed 24.
    if (value.length() < 4 || value.length() > 24) {
      return false;
    }
    return isDateTime(TIME_PATTERN_GROUP_LIST, value);
  }

  private static boolean isDateTime(List<Map<Pattern, String>> patternGroupList, String value) {
    if (StringUtils.isNotEmpty(value)) {
      // at least 3 digit
      boolean hasEnoughDigits = false;
      int digitCount = 0;
      for (int i = 0; i < value.length(); i++) {
        char ch = value.charAt(i);
        if (ch >= '0' && ch <= '9') {
          digitCount++;
          if (digitCount > 2) {
            hasEnoughDigits = true;
            break;
          }
        }
      }
      if (!hasEnoughDigits) {
        return false;
      }

      // Check the value with a list of regex patterns
      for (Map<Pattern, String> patternMap : patternGroupList) {
        for (Pattern parser : patternMap.keySet()) {
          try {
            if (parser.matcher(value).find()) {
              return true;
            }
          } catch (Exception e) {
            // ignore
          }
        }
      }
    }
    return false;
  }

  /**
   * Replace the value with date pattern string.
   *
   * @param value
   * @return date pattern string.
   */
  public static Set<String> datePatternReplace(String value) {
    return dateTimePatternReplace(DATE_PATTERN_GROUP_LIST, value);
  }

  /**
   * Replace the value with time pattern string.
   *
   * @param value
   * @return
   */
  public static Set<String> timePatternReplace(String value) {
    return dateTimePatternReplace(TIME_PATTERN_GROUP_LIST, value);
  }

  private static Set<String> dateTimePatternReplace(List<Map<Pattern, String>> patternGroupList, String value) {
    if (StringUtils.isEmpty(value)) {
      return Collections.singleton(StringUtils.EMPTY);
    }
    HashSet<String> resultSet = new HashSet<>();
    for (Map<Pattern, String> patternMap : patternGroupList) {
      for (Pattern parser : patternMap.keySet()) {
        if (parser.matcher(value).find()) {
          resultSet.add(patternMap.get(parser));
        }
      }
      if (!resultSet.isEmpty()) {
        return resultSet;
      }
    }
    return resultSet;
  }
}
