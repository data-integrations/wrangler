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

package io.cdap.directives.transformation;

import org.apache.commons.lang3.StringUtils;

/**
 * This class {@link Trimmer} operates on string to trim spaces across all UTF-8 character set of spaces.
 */
public final class Trimmer {

  /**
   * Defines all the white spaces for all the different UTF-8 Character set.
   */
  public static final String[] WHITESPACE_CHARS = new String[] {
    "\t" // CHARACTER TABULATION
    , "\n" // LINE FEED (LF)
    , '\u000B' + "" // LINE TABULATION
    , "\f" // FORM FEED (FF)
    , "\r" // CARRIAGE RETURN (CR)
    , " " // SPACE
    , '\u0085' + "" // NEXT LINE (NEL)
    , '\u00A0' + "" // NO-BREAK SPACE
    , '\u1680' + "" // OGHAM SPACE MARK
    , '\u180E' + "" // MONGOLIAN VOWEL SEPARATOR
    , '\u2000' + "" // EN QUAD
    , '\u2001' + "" // EM QUAD
    , '\u2002' + "" // EN SPACE
    , '\u2003' + "" // EM SPACE
    , '\u2004' + "" // THREE-PER-EM SPACE
    , '\u2005' + "" // FOUR-PER-EM SPACE
    , '\u2006' + "" // SIX-PER-EM SPACE
    , '\u2007' + "" // FIGURE SPACE
    , '\u2008' + "" // PUNCTUATION SPACE
    , '\u2009' + "" // THIN SPACE
    , '\u200A' + "" // HAIR SPACE
    , '\u2028' + "" // LINE SEPARATOR
    , '\u2029' + "" // PARAGRAPH SEPARATOR
    , '\u202F' + "" // NARROW NO-BREAK SPACE
    , '\u205F' + "" // MEDIUM MATHEMATICAL SPACE
    , '\u3000' + "" // IDEOGRAPHIC SPACE
  };

  /**
   * Remove trailing and leading characters which may be empty string,
   * space string,\t,\n,\r,\f...any space, break related characters.
   *
   * @param input - the input text.
   * @return String
   */
  public static String trim(String input) {
    if (StringUtils.isEmpty(input)) {
      return input;
    }

    String result = input;
    while (StringUtils.startsWithAny(result, WHITESPACE_CHARS)) {
      result = StringUtils.removeStart(result, result.substring(0, 1));
    }

    while (StringUtils.endsWithAny(result, WHITESPACE_CHARS)) {
      result = StringUtils.removeEnd(result, result.substring(result.length() - 1, result.length()));
    }
    return result;
  }

  /**
   * Remove leading characters which may be empty string,
   * space string,\t,\n,\r,\f...any space, break related characters.
   *
   * @param input - the input text.
   * @return String
   */
  public static String ltrim(String input) {
    if (StringUtils.isEmpty(input)) {
      return input;
    }

    String result = input;
    while (StringUtils.startsWithAny(result, WHITESPACE_CHARS)) {
      result = StringUtils.removeStart(result, result.substring(0, 1));
    }
    return result;
  }

  /**
   * Remove trailing characters which may be empty string,
   * space string,\t,\n,\r,\f...any space, break related characters.
   *
   * @param input - the input text.
   * @return String
   */
  public static String rtrim(String input) {
    if (StringUtils.isEmpty(input)) {
      return input;
    }

    String result = input;
    while (StringUtils.endsWithAny(result, WHITESPACE_CHARS)) {
      result = StringUtils.removeEnd(result, result.substring(result.length() - 1, result.length()));
    }
    return result;
  }
}
