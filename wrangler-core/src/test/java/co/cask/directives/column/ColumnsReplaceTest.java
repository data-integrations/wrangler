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
import co.cask.wrangler.api.RecipeException;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link ColumnsReplace}
 */
public class ColumnsReplaceTest {

  @Test
  public void testBasicColumnReplace() throws Exception {
    String[] directives = new String[] {
      "columns-replace s/^data_//g",
    };

    List<Row> rows = Arrays.asList(
      new Row("data_a", 1).add("data_b", 2).add("data_timestamp", 3).add("data_data_confuse", 4)
        .add("no_data", 5).add("whatever", 6)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("a", rows.get(0).getColumn(0));
    Assert.assertEquals("b", rows.get(0).getColumn(1));
    Assert.assertEquals("timestamp", rows.get(0).getColumn(2));
    Assert.assertEquals("data_confuse", rows.get(0).getColumn(3));
    Assert.assertEquals("no_data", rows.get(0).getColumn(4));
    Assert.assertEquals("whatever", rows.get(0).getColumn(5));
  }

  @Test(expected = RecipeException.class)
  public void testIncorrectSedExpression() throws Exception {
    String[] directives = new String[] {
      "columns-replace r/^data_//g", // Incorrect sed expression.
    };

    List<Row> rows = Arrays.asList(
      new Row("data_a", 1).add("data_b", 2).add("data_timestamp", 3).add("data_data_confuse", 4)
        .add("no_data", 5).add("whatever", 6)
    );

    TestingRig.execute(directives, rows);
  }
}
