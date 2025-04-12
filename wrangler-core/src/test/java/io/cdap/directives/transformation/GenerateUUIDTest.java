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
 * Tests {@link GenerateUUID}
 */
public class GenerateUUIDTest {

  @Test
  public void testUUIDGeneration() throws Exception {
    String[] directives = new String[] {
      "generate-uuid uuid",
    };

    List<Row> rows = Arrays.asList(
      new Row("value", "abc"),
      new Row("value", "xyz"),
      new Row("value", "Should be fine")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 3);
    Assert.assertEquals(2, rows.get(0).width());
    Assert.assertEquals("uuid", rows.get(1).getColumn(1));
    Assert.assertEquals("Should be fine", rows.get(2).getValue("value"));
  }

}
