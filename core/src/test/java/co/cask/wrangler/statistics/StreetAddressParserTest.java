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

import com.skovalenko.geocoder.address_parser.ParsedUsAddress;
import com.skovalenko.geocoder.address_parser.UnparsedAddress;
import com.skovalenko.geocoder.address_parser.us.UsAddressParser;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kewang on 5/30/17.
 */
public class StreetAddressParserTest {

  private StreetAddressParser parser = null;
  public StreetAddressParserTest() {
    parser = new StreetAddressParser();
  }

  @Test
  public void testParse() {
    /*
    String[] strs = parser.parseToArr("1905 N Lincoln Ave Apt 125 Urbana IL 61801");
    for (String s : strs) {
      System.out.println(s);
    }
    */

    //AddressParser.parseAddress("123 FISH AND GAME rd philadelphia pa 12345");

    /*
    UsAddressParser parser = new UsAddressParser();
    ParsedUsAddress address = parser.parse(new UnparsedAddress("1905 N Lincoln Ave Apt 125", "Urbana", "61801"));
    System.out.println(address);
    */
    Address desiredResult = new Address("1905", "N", "Lincoln", "Ave",
            "Apt", "125", "Urbana", "IL", "61801");
    String[] inputs = new String[]{
            "1905 N Lincoln Ave Apt 125 Urbana IL 61801",
            "1905 N Lincoln Ave Apt 125 Urbana IL",
            "1905 N Lincoln Ave Apt 125 Urbana",
            "1905 N Lincoln Ave Apt 125",
            "1905 N Lincoln Ave",
            "1905 N Lincoln",
            "1905 N",
            "1905",

            "61801",
            "IL 61801",
            "Urbana IL 61801",
            //"125 Urbana IL 61801",
            "Apt 125 Urbana IL 61801",
            "Ave Apt 125 Urbana IL 61801",
            "Lincoln Ave Apt 125 Urbana IL 61801",
            "N Lincoln Ave Apt 125 Urbana IL 61801"

    };


    for (String str : inputs) {
      Address result = parser.parse(str);

      Assert.assertTrue(result.sameAs(desiredResult));

      //System.out.println("Input: " + str);
      //System.out.println(result + "\n");
    }

  }
}
