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

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
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

    List<Record> records = Arrays.asList(
      new Record("body", "one two three four five six seven eight")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals(8, records.get(0).length());
    Assert.assertEquals("one", records.get(0).getValue("one"));
    Assert.assertEquals("two", records.get(0).getValue("two"));
    Assert.assertEquals("three", records.get(0).getValue("three"));
    Assert.assertEquals("four five six seven eight", records.get(0).getValue("four"));
    Assert.assertEquals("one", records.get(0).getValue("five"));
    Assert.assertEquals("one", records.get(0).getValue("six"));
    Assert.assertEquals("one t", records.get(0).getValue("seven"));
  }
}
