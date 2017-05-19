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

package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.TestUtil;
import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Trim}
 */
public class TrimTest {

  @Test
  public void testSingleWordTrim() throws Exception {
    String expected = "TITLE";

    String[] directives = new String[] {
      "trim body",
    };

    List<Record> records = Arrays.asList(
      new Record("body", expected),
      new Record("body", "  " + expected),
      new Record("body", expected + "  "),
      new Record("body", " " + expected + " "),
      new Record("body", "  " + expected + "  "),
      new Record("body", "\t\r" + expected + "\r\t")
    );

    records = TestUtil.run(directives, records);
    Assert.assertEquals(records.size(), records.size());
    for(int i = 0; i < records.size(); ++i) {
      Assert.assertEquals(expected, records.get(i).getValue("body"));
    }
  }

  @Test
  public void testSentenceTrim() throws Exception {
    String expected = "TITLE IS TITLE";

    String[] directives = new String[] {
      "trim body",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "TITLE IS TITLE"),
      new Record("body", "    TITLE IS TITLE"),
      new Record("body", "TITLE IS TITLE    "),
      new Record("body", " TITLE    IS TITLE "),
      new Record("body", "   TITLE IS TITLE   "),
      new Record("body", "\t TITLE IS TITLE \t"),
      new Record("body", "\t" + expected),
      new Record("body", expected + "\t"),
      new Record("body", '\u0009' + expected),
      new Record("body", "\r" + expected),
      new Record("body", expected + "\r"),
      new Record("body", '\u2004' + expected),
      new Record("body", expected + '\u2004'),
      new Record("body", '\u2005' + expected),
      new Record("body", expected + '\u2005'),
      new Record("body", '\u2006' + expected),
      new Record("body", expected + '\u2006'),
      new Record("body", '\u3000' + expected),
      new Record("body", '\t' + expected + '\u3000' + '\u3000' + '\t' + '\r')
    );

    records = TestUtil.run(directives, records);
    Assert.assertEquals(records.size(), records.size());
    Assert.assertEquals("TITLE IS TITLE", records.get(0).getValue("body"));
    Assert.assertEquals("TITLE IS TITLE", records.get(1).getValue("body"));
    Assert.assertEquals("TITLE IS TITLE", records.get(2).getValue("body"));
    Assert.assertEquals("TITLE    IS TITLE", records.get(3).getValue("body"));
    Assert.assertEquals("TITLE IS TITLE", records.get(4).getValue("body"));
    for(int i = 5; i < records.size(); ++i) {
      Assert.assertEquals(expected, records.get(i).getValue("body"));
    }
  }
}