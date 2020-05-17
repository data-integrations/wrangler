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
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link io.cdap.directives.column.Copy}
 */
public class CopyTest {

  @Test
  public void testBasicCopy() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "copy body_1 name"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "A,B,1"),
      new Row("body", "D,E,2"),
      new Row("body", "G,H,3")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 3);
    Assert.assertEquals(5, rows.get(0).width()); // should have copied to another column
    Assert.assertEquals("A", rows.get(0).getValue("name")); // Should have copy of 'A'
    Assert.assertEquals("D", rows.get(1).getValue("name")); // Should have copy of 'D'
    Assert.assertEquals("G", rows.get(2).getValue("name")); // Should have copy of 'G'
    Assert.assertEquals(rows.get(0).getValue("name"), rows.get(0).getValue("body_1"));
    Assert.assertEquals(rows.get(1).getValue("name"), rows.get(1).getValue("body_1"));
    Assert.assertEquals(rows.get(2).getValue("name"), rows.get(2).getValue("body_1"));
  }

  @Test(expected = RecipeException.class)
  public void testCopyToExistingColumn() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "copy body_1 body_2"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "A,B,1"),
      new Row("body", "D,E,2"),
      new Row("body", "G,H,3")
    );

    rows = TestingRig.execute(directives, rows);
  }

  @Test
  public void testForceCopy() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "copy body_1 body_2 true"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "A,B,1"),
      new Row("body", "D,E,2"),
      new Row("body", "G,H,3")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 3);
    Assert.assertEquals(4, rows.get(0).width()); // should have copied to another column
    Assert.assertEquals("A", rows.get(0).getValue("body_2")); // Should have copy of 'A'
    Assert.assertEquals("D", rows.get(1).getValue("body_2")); // Should have copy of 'D'
    Assert.assertEquals("G", rows.get(2).getValue("body_2")); // Should have copy of 'G'
    Assert.assertEquals(rows.get(0).getValue("body_2"), rows.get(0).getValue("body_1"));
    Assert.assertEquals(rows.get(1).getValue("body_2"), rows.get(1).getValue("body_1"));
    Assert.assertEquals(rows.get(2).getValue("body_2"), rows.get(2).getValue("body_1"));
  }

}
