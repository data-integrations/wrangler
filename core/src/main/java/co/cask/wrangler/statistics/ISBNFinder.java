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
import org.apache.commons.validator.routines.ISBNValidator;

import java.util.ArrayList;

/**
 * ISBN string detector
 */
public class ISBNFinder {

  //ISBN validator from external library
  private ISBNValidator validator;

  public ISBNFinder() {
    validator = new ISBNValidator();
  }

  /**
   * Check if the input string is ISBN
   * @param str
   * @return
   */
  public boolean isISBN(String str) {
    //Need to format the input before validating

    //Extract only digits from string, without other characters
    ArrayList<Integer> intList = new ArrayList<>();
    char [] s = str.toCharArray();
    for (int i = 0; i < s.length; i ++) {
      char c = s[i];
      if (Character.isDigit(c)) {
        int value = Character.getNumericValue(c);
        intList.add(value);
      }
    }

    int len = intList.size();

    if (len == 10) {
      //format the digits as ISBN-10 form, "0-345-50113-6"
      StringBuilder sb = new StringBuilder();
      sb.append(intList.get(0));
      sb.append('-');
      for (int i = 1; i < 4; i ++) {
        sb.append(intList.get(i));
      }
      sb.append('-');
      for (int i = 4; i < 9; i ++) {
        sb.append(intList.get(i));
      }
      sb.append('-');
      sb.append(intList.get(9));
      String code = sb.toString();
      return validator.isValid(code);
    }

    else if (len == 13) {
      //format the digits as ISBN-13 form, "978-0-345-50113-4"
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 3; i ++) {
        sb.append(intList.get(i));
      }
      sb.append('-');
      sb.append(intList.get(3));
      sb.append('-');
      for (int i = 4; i < 7; i ++) {
        sb.append(intList.get(i));
      }
      sb.append('-');
      for (int i = 7; i < 12; i ++) {
        sb.append(intList.get(i));
      }
      sb.append('-');
      sb.append(intList.get(12));
      String code = sb.toString();
      return validator.isValid(code);
    }
    else {
      return false;
    }
  }
}
