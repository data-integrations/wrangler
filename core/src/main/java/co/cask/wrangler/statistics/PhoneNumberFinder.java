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

import au.com.bytecode.opencsv.CSVReader;
import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Phone number string detector
 */
public class PhoneNumberFinder {
  private PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
  private final String COUNTRY_CODE_FILE = "country_codes.csv";
  private List<String> countryCodes = null;

  public PhoneNumberFinder() {
    //read country codes from csv file, so we can be flexible on which countries' phone number to recognize
    countryCodes = readFromCsv(COUNTRY_CODE_FILE);
  }

  /**
   * Check if a string is valid phone number in certain given countries
   * @param str
   * @return
   */
  public boolean isValidPhone(String str) {
    return isValidPhoneHelper(str, countryCodes);
  }

  private List<String> readFromCsv(String fileName) {
    List<String> list = new ArrayList<>();
    CSVReader reader = null;
    try {
      BufferedReader bReader = new BufferedReader(new InputStreamReader(
              this.getClass().getResourceAsStream("/" + fileName)));
      reader = new CSVReader(bReader);
      String [] nextLine;
      while ((nextLine = reader.readNext()) != null) {
        for (String str : nextLine) {
          list.add(str);
        }
      }
      return list;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  private boolean isValidPhoneHelper(String str, List<String> countries) {
    for (String country : countries) {
      try {
        Phonenumber.PhoneNumber number = phoneUtil.parseAndKeepRawInput(str, country);
        boolean isNumberValid = phoneUtil.isValidNumber(number);
        if (isNumberValid) {
          return true;
        }
      } catch (NumberParseException e) {
        //e.printStackTrace();
      }
    }
    return false;
  }
}
