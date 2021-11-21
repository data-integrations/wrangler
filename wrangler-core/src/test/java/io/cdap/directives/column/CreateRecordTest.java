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
 * Tests {@link CreateRecord}
 */
public class CreateRecordTest {

  /**
   * | col1 | col2 | col3
   * | "A"  | "B"  | "C"
   *
   * Directive
   * create-record :result, :col1, :col2, :col3
   *
   * Result
   *
   * | col1 | col2 | col3 | result
   * | "A"  | "B"  | "C" |  {col1: "A", col2: "B", col3: "C"}
   *
   * @throws Exception
   */
  @Test
  public void testBasicCase() throws Exception {
    String[] directives = new String[] {
      "create-record :result :col1, :col2, :col3",
    };

    Row newObjectRow = new Row("col1", "A")
      .add("col2", "B")
      .add("col3", "C");


    List<Row> rows = Arrays.asList(
      new Row(newObjectRow)
    );

    rows = TestingRig.execute(directives, rows);

    Row expectedResultRow = new Row(newObjectRow).add("result", newObjectRow);
    Assert.assertTrue(expectedResultRow.equals(rows.get(0)));
  }

}
