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
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link PhoneNumberFinder}
 */
public class PhoneNumberTest {
  private PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
  private final String NUMBERS_FILE = "phone.csv";
  private final String NON_NUMBERS_FILE = "mock_one_line.csv";
  private final PhoneNumberFinder finder;
  private List<String> phoneNumbers = null;
  private List<String> nonPhoneNumbers = null;

  public PhoneNumberTest() {
    //read csv numbers
    phoneNumbers = readFromCsv(NUMBERS_FILE);
    nonPhoneNumbers = readFromCsv(NON_NUMBERS_FILE);
    finder = new PhoneNumberFinder();
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

  /**
   * Test phone number validating using phone numbers from a csv file
   * @throws Exception
   */
  @Test
  public void testValidPhoneNumber() throws Exception {
    for (String str : phoneNumbers) {
      Assert.assertTrue(finder.isValidPhone(str));
    }
  }

  //TODO: Due to too many phone number formats to match (many countries), it tends to recognize other content as phone numbers
  @Ignore
  @Test
  public void testInvalidPhoneNumber() throws Exception {

    for (String str : nonPhoneNumbers) {
      /*
      boolean valid = finder.isValidPhone(str);
      if (valid) {
        System.out.println(str);
      }
      */
      Assert.assertFalse(finder.isValidPhone(str));
    }
  }
}
