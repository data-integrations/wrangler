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
 * Tests for {@link ISBNFinder}
 */
public class ISBNFinderTest {
  String [] codeList;
  String [] nonCodeList;
  ISBNFinder finder;

  public ISBNFinderTest() {
    codeList = new String[]{"0-345-50113-6", "978-0-345-50113-4", "978-3-16-148410-0", "978-0-34-580348-1",
    "172411764-5", "6282527813", "978-0345803481"};
    nonCodeList = new String[] {"217-333-1346", "61820", "001-217-333-5988"};
    finder = new ISBNFinder();
  }

  @Test
  public void testISBNFinder() {
    for (String code : codeList) {
      Assert.assertTrue(finder.isISBN(code));

    }
    for (String nonCode : nonCodeList) {
      Assert.assertFalse(finder.isISBN(nonCode));
    }
  }

}
