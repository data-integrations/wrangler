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
 * Tests {@link Lower}
 */
public class LowerTest {

  @Test
  public void testSingleWordLowerCasing() throws Exception {
    String[] directives = new String[] {
      "lowercase body",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "TITLE"),
      new Row("body", "tiTLE"),
      new Row("body", "title"),
      new Row("body", "TitlE")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(4, rows.size());
    Assert.assertEquals("title", rows.get(0).getValue("body"));
    Assert.assertEquals("title", rows.get(1).getValue("body"));
    Assert.assertEquals("title", rows.get(2).getValue("body"));
    Assert.assertEquals("title", rows.get(3).getValue("body"));
  }

  @Test
  public void testSentenceLowercasing() throws Exception {
    String[] directives = new String[] {
      "lowercase body",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "TITLE is TITLE"),
      new Row("body", "tiTLE IS tItle"),
      new Row("body", "title is title"),
      new Row("body", "TitlE Is TiTLE")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(4, rows.size());
    Assert.assertEquals("title is title", rows.get(0).getValue("body"));
    Assert.assertEquals("title is title", rows.get(1).getValue("body"));
    Assert.assertEquals("title is title", rows.get(2).getValue("body"));
    Assert.assertEquals("title is title", rows.get(3).getValue("body"));
  }
}
