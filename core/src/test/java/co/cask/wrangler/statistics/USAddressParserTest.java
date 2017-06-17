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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link USAddressParser}
 */
@Ignore
public class USAddressParserTest {
  private static final Logger LOG = LoggerFactory.getLogger(USAddressParserTest.class);
  private USAddressParser parser = null;
  public USAddressParserTest() {
    parser = new USAddressParser();
  }

  @Test
  public void testStandardParse() {
    USAddress standardResult = new USAddress("1905", "N", "Lincoln", "Ave",
            "Apt", "125", "Urbana", "IL", "61801");
    String[] standardInputs = new String[]{
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
            "Apt 125 Urbana IL 61801",
            "Ave Apt 125 Urbana IL 61801",
            "Lincoln Ave Apt 125 Urbana IL 61801",
            "N Lincoln Ave Apt 125 Urbana IL 61801"
    };

    for (String str : standardInputs) {
      USAddress result = parser.parse(str);
      LOG.info("Input: " + str);
      LOG.info(result + "\n");
      Assert.assertTrue(result.sameAs(standardResult));
    }
  }

  @Test
  public void testNonStandardParse() {
    String[] nonStandardInputs = new String[]{
            "5000 Forbes Ave Pittsburgh PA 15213",
            "6500 Soquel Dr",
            "419 Boston Ave Medford MA 02155",
            "Madison WI 53706",
            "450 Serra Mall Stanford CA 94305",
            "6500 Soquel Dr Aptos CA 95003"
    };

    for (String str : nonStandardInputs) {
      USAddress result = parser.parse(str);
      LOG.info("Input: " + str);
      LOG.info(result + "\n");
    }
  }
}
