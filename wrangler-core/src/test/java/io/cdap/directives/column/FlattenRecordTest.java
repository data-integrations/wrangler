/*
 *  Copyright © 2017-2021 Cask Data, Inc.
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
 * Tests {@link FlattenRecordTest}
 */
public class FlattenRecordTest {

  /**
   * struct
   * {col1: "A", col2: "B", col3: "C"}
   *
   * Directive
   * explode-record :struct
   *
   * Result
   *
   * struct                            | struct_col1 | struct_col2 | struct_col3
   * {col1: "A", col2: "B", col3: "C"} | "A"         | "B"         | "C"
   *
   * @throws Exception
   */
  @Test
  public void testBasicCase() throws Exception {
    String[] directives = new String[] {
      "flatten-record :struct",
    };

    Row newObjectRow = new Row("col1", "A")
      .add("col2", "B")
      .add("col3", "C");


    List<Row> rows = Arrays.asList(
      new Row("struct", newObjectRow)
    );

    rows = TestingRig.execute(directives, rows);

    Row expectedResultRow = new Row("struct", newObjectRow)
      .add("struct_col1", "A")
      .add("struct_col2", "B")
      .add("struct_col3", "C");

    Assert.assertEquals(expectedResultRow, rows.get(0));
    Assert.assertEquals(rows.size(), 1);
  }


  @Test
  public void testBasicCase2() throws Exception {
    String[] directives = new String[] {
      "flatten-record :struct",
      "flatten-record :struct_child1",
    };

    Row childRow = new Row("col1", "A")
      .add("col2", "B")
      .add("col3", "C");

    Row newObjectRow = new Row("child1", childRow);

    List<Row> rows = Arrays.asList(
      new Row("struct", newObjectRow)
    );

    rows = TestingRig.execute(directives, rows);

    Row expectedResultRow = new Row("struct", newObjectRow)
      .add("struct_child1", childRow)
      .add("struct_child1_col1", "A")
      .add("struct_child1_col2", "B")
      .add("struct_child1_col3", "C");

    Assert.assertEquals(expectedResultRow, rows.get(0));
    Assert.assertEquals(rows.size(), 1);
  }

}
