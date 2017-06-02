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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang.math.NumberUtils.isNumber;

/**
 * Street address detector
 * Recognize address components like city, state, street name, street number etc.
 * 1905 N Lincoln Ave Apt 125 Urbana IL 61801
 */
public class AddressFinder {

  //not in use because address parser is not accurate
  private StreetAddressParser addressParser;

  //private Set<String> words;
  //private String DICT_FILE = "words.txt";


  private String US_REGEX_PATTERN = "\\d+[ ](?:[A-Za-z0-9.-]+[ ]?)+(?:Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Parkway|Way|Circle|Plaza|Dr|Rd|Blvd|Ln|St|)\\.?";


  private String US_ZIP_REGEX = "^\\d{5}(-\\d{4})?$";
  private String US_STATE_REGEX = "^(AL|Alabama|AK|Alaska|AZ|Arizona|AR|Arkansas|CA|California|CO|Colorado|CT|Connecticut|DE|Delaware|FL|Florida|GA|Georgia|HI|Hawaii|ID|Idaho|IL|Illinois|IN|Indiana|IA|Iowa|KS|Kansas|KY|Kentucky|LA|Louisiana|ME|Maine|MD|Maryland|MA|Massachusetts|MI|Michigan|MN|Minnesota|MS|Mississippi|MO|Missouri|MT|Montana|NE|Nebraska|NV|Nevada|NH|New Hampshire|NJ|New Jersey|NM|New Mexico|NY|New York|NC|North Carolina|ND|North Dakota|OH|Ohio|OK|Oklahoma|OR|Oregon|PA|Pennsylvania|RI|Rhode Island|SC|South Carolina|SD|South Dakota|TN|Tennessee|TX|Texas|UT|Utah|VT|Vermont|VA|Virginia|WA|Washington|WV|West Virginia|WI|Wisconsin|WY|Wyoming)$";
  private String STREET_SUFFIX_REGEX = "^(Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Parkway|Way|Circle|Plaza|Mall|Dr|Rd|Blvd|Ln|St)$";
  private String STREET_PREFIX_REGEX = "^(North|South|East|West|north|south|east|west|NORTH|SOUTH|WEST|EAST|N|S|E|W)$";
  private String UNIT_NAME_REGEX = "^(Room|ROOM|room|RM|Apt|APT|apt|Unit|UNIT|unit)$";


  public AddressFinder() {

    //words = readFromCsv(DICT_FILE);
  }


  /**
   * Check if the input is in valid US address format
   * @param str
   * @return
   */
  public boolean isUSAddress(String str) {

    return matchKeyWords(str);
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

  public boolean isUSZipCode (String str) {
    Pattern pattern = Pattern.compile(US_ZIP_REGEX);
    Matcher matcher = pattern.matcher(str);
    return matcher.matches();
  }

  public boolean isUSState(String str) {
    Pattern pattern = Pattern.compile(US_STATE_REGEX);
    Matcher matcher = pattern.matcher(str);
    return matcher.matches();
  }

  public boolean isCity(String str) {

    return false;

    //comment out for debug

    /*
    if (isNumber(str)) {
      return false;
    }
    if (!words.contains(str)) {
      return true;
    }
    else {
      return false;
    }
    */
  }

  public boolean isSuffix(String str) {
    Pattern pattern = Pattern.compile(STREET_SUFFIX_REGEX);
    Matcher matcher = pattern.matcher(str);
    return matcher.matches();
  }

  public boolean isPrefix(String str) {
    Pattern pattern = Pattern.compile(STREET_PREFIX_REGEX);
    Matcher matcher = pattern.matcher(str);
    return matcher.matches();
  }

  public boolean isUnitName(String str) {
    Pattern pattern = Pattern.compile(UNIT_NAME_REGEX);
    Matcher matcher = pattern.matcher(str);
    return matcher.matches();
  }

  private Set<String> readFromCsv(String fileName) {
    Set<String> set = new HashSet<>();
    CSVReader reader = null;
    try {
      BufferedReader bReader = new BufferedReader(new InputStreamReader(
              this.getClass().getResourceAsStream("/" + fileName)));
      reader = new CSVReader(bReader);
      String [] nextLine;
      while ((nextLine = reader.readNext()) != null) {
        for (String str : nextLine) {
          set.add(str);
        }
      }
      return set;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
