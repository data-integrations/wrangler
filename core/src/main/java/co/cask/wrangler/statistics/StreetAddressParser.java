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

import org.apache.commons.lang3.builder.ToStringExclude;

import static org.apache.commons.lang.math.NumberUtils.isNumber;

/**
 * Created by kewang on 5/30/17.
 */
public class StreetAddressParser {

  private AddressFinder finder;

  public StreetAddressParser() {
    finder = new AddressFinder();
  }

  public Address parse(String str) {
    return parseToAddress(parseToArr(str));
  }

  private String[] parseToArr(String str) {
    //all spaces
    String [] strArray = str.split(" ");
    return strArray;
  }

  private Address parseToAddress (String [] strArray) {
    Address address = new Address();
    int streetNameIndex = -1;
    if (strArray.length > 0) {
      String firstStr = strArray[0];
      int firstSymbolIndex;
      if (isNumber(firstStr)) {
        if (finder.isUSZipCode(firstStr)) {
          address.setZipCode(firstStr);
        }
        else {
          address.setStreetNumber(firstStr);
        }
        firstSymbolIndex = 1;
      }
      else {
        firstSymbolIndex = 0;
      }
      for (int i = strArray.length - 1; i >= firstSymbolIndex; i--) {
        String str = strArray[i];
        if (finder.isUSZipCode(str)) {
          if (address.getZipCode() == null) {
            address.setZipCode(str);
          }
        } else if (finder.isUnitName(str)) {
          if (address.getUnitName() == null) {
            address.setUnitName(str);
          }
        } else if (finder.isUSState(str)) {
          if (address.getState() == null) {
            address.setState(str);
          }
        } else if (finder.isCity(str)) {
          if (address.getCity() == null) {
            address.setCity(str);
          }
        }
        //1905 N Lincoln Ave Apt 125 Urbana IL 61801
        else if (isNumber(str)) {
          if (address.getUnitNumber() == null) {
            address.setUnitNumber(str);
          } else if (address.getStreetNumber() == null) {
            address.setStreetNumber(str);
          }
        } else if (finder.isSuffix(str)) {
          if (address.getStreetSuffix() == null) {
            address.setStreetSuffix(str);
            if (i > 0) {
              streetNameIndex = i - 1;
            }
          }
        } else if (finder.isPrefix(str)) {
          if (address.getStreetPrefix() == null) {
            address.setStreetPrefix(str);
          }
        }
      }
      if (streetNameIndex >= 0) {
        address.setStreetName(strArray[streetNameIndex]);
      }
    }

    return address;
  }

}
