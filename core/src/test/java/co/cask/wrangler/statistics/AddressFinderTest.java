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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link AddressFinder}, using only US addresses for now
 */
public class AddressFinderTest {
  private AddressFinder finder;
  private String [] addressList;

  public AddressFinderTest() {
    finder = new AddressFinder();
    //Complicated International address or unformatted US address is not recognizable
    addressList = new String[] {"1905 N. Lincoln Ave Apt 125, Urbana IL 61801",
            "510 E. Clark St Apt 26, Champaign IL 61820",
            "150 Grant Ave, Palo Alto, CA 94306", "92 Wucheng Rd, Xiaodian Qu, Taiyuan Shi, Shanxi Sheng, China",
            "Pokfulam, Hong Kong", "Hauz Khas, New Delhi, Delhi 110016, India", "Hauz Khas, New Delhi, Delhi 110016",
            "Hauz Khas, New Delhi, Delhi", "Hauz Khas"};
  }

  @Test
  public void testAddressFinder() {
    String [] usAddressList = new String[] {
            "478 Macpherson Drive",
            "3595 Welch Circle",
            "91781 Hermina Plaza",
            "9 Garrison Way",
            "6707 Eagan Street",
            "2566 Westend Parkway"
    };
    for (String str : usAddressList) {
      Assert.assertTrue(finder.isUSAddress(str));
    }
  }
}
