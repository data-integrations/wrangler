/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.column;

import co.cask.wrangler.TestingRig;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link ChangeColCaseNames}
 */
public class ChangeColCaseNamesTest {

  @Test
  public void testColumnCaseChanges() throws Exception {
    String[] directives = new String[] {
      "change-column-case lower",
    };

    List<Row> rows = Arrays.asList(
      new Row("Url", "1").add("Fname", "2").add("LName", "3").add("ADDRESS", "4")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("url", rows.get(0).getColumn(0));
  }
}
