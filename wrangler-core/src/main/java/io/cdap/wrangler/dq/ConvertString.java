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

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Useful String conversion operations.
 */
public class ConvertString {

  public static final String[] WHITESPACE_CHARS = new String[] { "\t" // CHARACTER TABULATION
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

  private String repeatChar = null;
  private Pattern removeRepeatCharPattern = null;
  private Pattern removeWhiteSpacesPattern = null;

  /**
   * This constructor is used to some cases but except removing a specify repeated String
   * {@link #removeRepeatedChar(String)}.
   */
  public ConvertString() {
    this(null);
  }

  /**
   *
   * This constructor is used to remove a specify repeated String {@link #removeRepeatedChar(String)} .
   *
   * @param repeatStr it is a repeat String
   */
  public ConvertString(String repeatStr) {
    this.repeatChar = repeatStr;
    if (!StringUtils.isEmpty(repeatStr)) {
      removeRepeatCharPattern = Pattern.compile("(" + repeatStr + ")+");
    }
    removeWhiteSpacesPattern = Pattern.compile("([\\s\\u0085\\p{Z}])\\1+");
  }

  /**
   * Remove trailing and leading characters which may be empty string,
   * space string,\t,\n,\r,\f...any space, break related
   * characters.
   *
   * @param input - the input text.
   * @return String
   */
  public String removeTrailingAndLeadingWhitespaces(String input) {
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
   * Remove trailing and leading characters and the remove Character is whitespace only. <br/>
   * <br/>
   * Note: this is not equals input.trim() for example: <br/>
   * when the input is ("\t" + "abc "), this method will return ("\t" + "abc"),<br/>
   * but for trim method will return "abc"
   *
   * @param input - the input text.
   * @return String
   */
  public String removeTrailingAndLeading(String input) {
    return removeTrailingAndLeading(input, " "); //$NON-NLS-1$
  }

  /**
   * Remove trailing and leading characters.
   *
   * @param input - the input text.
   * @param character - the remove character.
   * @return String.
   */
  public String removeTrailingAndLeading(String input, String character) {
    if (StringUtils.isEmpty(input) || StringUtils.isEmpty(character)) {
      return input;
    }

    String result = input;

    while (result.startsWith(character)) {
      result = StringUtils.removeStart(result, character);
    }
    while (result.endsWith(character)) {
      result = StringUtils.removeEnd(result, character);
    }
    return result;
  }

  /**
   *
   * Remove consecutive repeated characters by a specified char.
   *
   * @param input the source String
   * @return the string with the source string removed if found
   */
  public String removeRepeatedChar(String input) {
    if (StringUtils.isEmpty(input) || StringUtils.isEmpty(repeatChar) || removeRepeatCharPattern == null) {
      return input;
    }
    Matcher matcher = removeRepeatCharPattern.matcher(input);
    return matcher.replaceAll(repeatChar);
  }

  /**
   *
   * Remove all repeated white spaces which include all strings in {@link #WHITESPACE_CHARS}
   * like as " ","\n","\r","\t".
   *
   * <pre>
   * removeRepeatedWhitespaces(null) = null
   * removeRepeatedWhitespaces("")   = ""
   * removeRepeatedWhitespaces("a  back\t\t\td") = "a back\td"
   * </pre>
   *
   * @param input input the source String
   * @return the string removed all whiteSpaces
   */
  public String removeRepeatedWhitespaces(String input) {
    if (StringUtils.isEmpty(input) || removeWhiteSpacesPattern == null) {
      return input;
    }
    Matcher matcher = removeWhiteSpacesPattern.matcher(input);
    return matcher.replaceAll("$1");
  }

}
