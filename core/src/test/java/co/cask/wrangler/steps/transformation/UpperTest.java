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
 * Tests {@link Upper}
 */
public class UpperTest {

  @Test
  public void testSingleWordLowerCasing() throws Exception {
    String[] directives = new String[] {
      "uppercase body",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "TITLE"),
      new Record("body", "tiTLE"),
      new Record("body", "title"),
      new Record("body", "TitlE")
    );

    records = TestUtil.run(directives, records);

    Assert.assertEquals(4, records.size());
    Assert.assertEquals("TITLE", records.get(0).getValue("body"));
    Assert.assertEquals("TITLE", records.get(1).getValue("body"));
    Assert.assertEquals("TITLE", records.get(2).getValue("body"));
    Assert.assertEquals("TITLE", records.get(3).getValue("body"));
  }

  @Test
  public void testSentenceLowercasing() throws Exception {
    String[] directives = new String[] {
      "uppercase body",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "TITLE is TITLE"),
      new Record("body", "tiTLE IS tItle"),
      new Record("body", "title is title"),
      new Record("body", "TitlE Is TiTLE")
    );

    records = TestUtil.run(directives, records);
    Assert.assertEquals(4, records.size());
    Assert.assertEquals("TITLE IS TITLE", records.get(0).getValue("body"));
    Assert.assertEquals("TITLE IS TITLE", records.get(1).getValue("body"));
    Assert.assertEquals("TITLE IS TITLE", records.get(2).getValue("body"));
    Assert.assertEquals("TITLE IS TITLE", records.get(3).getValue("body"));
  }
}