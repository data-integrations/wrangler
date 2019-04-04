/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives.transformation;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
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

    List<Row> rows = Arrays.asList(
      new Row("body", expected),
      new Row("body", "  " + expected),
      new Row("body", expected + "  "),
      new Row("body", " " + expected + " "),
      new Row("body", "  " + expected + "  "),
      new Row("body", "\t\r" + expected + "\r\t")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(rows.size(), rows.size());
    for (int i = 0; i < rows.size(); ++i) {
      Assert.assertEquals(expected, rows.get(i).getValue("body"));
    }
  }

  @Test
  public void testSentenceTrim() throws Exception {
    String expected = "TITLE IS TITLE";

    String[] directives = new String[] {
      "trim body",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "TITLE IS TITLE"),
      new Row("body", "    TITLE IS TITLE"),
      new Row("body", "TITLE IS TITLE    "),
      new Row("body", " TITLE    IS TITLE "),
      new Row("body", "   TITLE IS TITLE   "),
      new Row("body", "\t TITLE IS TITLE \t"),
      new Row("body", "\t" + expected),
      new Row("body", expected + "\t"),
      new Row("body", '\u0009' + expected),
      new Row("body", "\r" + expected),
      new Row("body", expected + "\r"),
      new Row("body", '\u2004' + expected),
      new Row("body", expected + '\u2004'),
      new Row("body", '\u2005' + expected),
      new Row("body", expected + '\u2005'),
      new Row("body", '\u2006' + expected),
      new Row("body", expected + '\u2006'),
      new Row("body", '\u3000' + expected),
      new Row("body", '\t' + expected + '\u3000' + '\u3000' + '\t' + '\r')
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(rows.size(), rows.size());
    Assert.assertEquals("TITLE IS TITLE", rows.get(0).getValue("body"));
    Assert.assertEquals("TITLE IS TITLE", rows.get(1).getValue("body"));
    Assert.assertEquals("TITLE IS TITLE", rows.get(2).getValue("body"));
    Assert.assertEquals("TITLE    IS TITLE", rows.get(3).getValue("body"));
    Assert.assertEquals("TITLE IS TITLE", rows.get(4).getValue("body"));
    for (int i = 5; i < rows.size(); ++i) {
      Assert.assertEquals(expected, rows.get(i).getValue("body"));
    }
  }
}
