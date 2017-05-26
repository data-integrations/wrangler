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
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kewang on 5/25/17.
 */
public class ISBNFinderTest {
  String [] codeList;
  ISBNFinder finder;

  public ISBNFinderTest() {
    codeList = new String[]{"0-345-50113-6", "978-0-345-50113-4", "978-3-16-148410-0", "978-0-34-580348-1"};
    finder = new ISBNFinder();
  }

  @Test
  public void testISBNFinder() {
    for (String code : codeList) {
      Assert.assertTrue(finder.isISBN(code));
    }
  }

}
