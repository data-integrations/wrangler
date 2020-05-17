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
 * Tests {@link CharacterCut}
 */
public class CharacterCutTest {

  @Test
  public void testBasicCharacterCut() throws Exception {
    String[] directives = new String[] {
      "cut-character body one 1-3",
      "cut-character body two 5-7",
      "cut-character body three 9-13",
      "cut-character body four 15-",
      "cut-character body five 1,2,3",
      "cut-character body six -3",
      "cut-character body seven 1,2,3-5",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "one two three four five six seven eight")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(8, rows.get(0).width());
    Assert.assertEquals("one", rows.get(0).getValue("one"));
    Assert.assertEquals("two", rows.get(0).getValue("two"));
    Assert.assertEquals("three", rows.get(0).getValue("three"));
    Assert.assertEquals("four five six seven eight", rows.get(0).getValue("four"));
    Assert.assertEquals("one", rows.get(0).getValue("five"));
    Assert.assertEquals("one", rows.get(0).getValue("six"));
    Assert.assertEquals("one t", rows.get(0).getValue("seven"));
  }

  @Test
  public void testDollarIncludedInString() throws Exception {
    String[] directives = new String[] {
      "cut-character body value 2-"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "$734.77")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("734.77", rows.get(0).getValue("value"));
  }
}
