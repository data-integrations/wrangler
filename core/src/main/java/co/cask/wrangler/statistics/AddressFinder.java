/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.statistics;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Street address string detector
 */
public class AddressFinder {
  private String US_REGEX_PATTERN = "\\d+[ ](?:[A-Za-z0-9.-]+[ ]?)+(?:Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Parkway|Way|Circle|Plaza|Dr|Rd|Blvd|Ln|St|)\\.?";

  /**
   * Check if the input is in valid US address format
   * @param str
   * @return
   */
  public boolean isUSAddress(String str) {
    return matchKeyWords(str);
    //TODO: Need more powerful way to validate address
  }

  /**
   * Match string input with regular expression pattern for street address
   * @param str
   * @return
   */
  private boolean matchKeyWords(String str) {
    Pattern pattern = Pattern.compile(US_REGEX_PATTERN);
    Matcher matcher = pattern.matcher(str);
    return matcher.matches();
  }

}
