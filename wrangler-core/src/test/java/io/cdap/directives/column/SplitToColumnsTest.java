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

package io.cdap.directives.column;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SplitToColumns}
 */
public class SplitToColumnsTest {
  @Test
  public void testSplitToColumns() throws Exception {
    String[] directives = new String[] {
      "split-to-columns body \\n",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "AABBCDE\nEEFFFF")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("AABBCDE", rows.get(0).getValue("body_1"));
    Assert.assertEquals("EEFFFF", rows.get(0).getValue("body_2"));
  }
}

