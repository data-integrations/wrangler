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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link CleanseColumnNames}
 */
public class CleanseColumnNamesTest {

  @Test
  public void testColumnCleanse() throws Exception {
    String[] directives = new String[] {
      "cleanse-column-names",
    };

    List<Row> rows = Collections.singletonList(
      new Row("COL1", "1").add("col:2", "2").add("Col3", "3").add("COLUMN4", "4").add("col!5", "5")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("col1", rows.get(0).getColumn(0));
    Assert.assertEquals("col_2", rows.get(0).getColumn(1));
    Assert.assertEquals("col3", rows.get(0).getColumn(2));
    Assert.assertEquals("column4", rows.get(0).getColumn(3));
    Assert.assertEquals("col_5", rows.get(0).getColumn(4));
  }

  @Test
  public void testGetOutputSchemaForCleansedColumns() throws Exception {
    String[] directives = new String[] {
      "cleanse-column-names",
    };
    List<Row> rows = Collections.singletonList(
      new Row("COL1", 1).add("col:2", new BigDecimal("143235.016"))
    );
    Schema inputSchema = Schema.recordOf(
      "inputSchema",
      Schema.Field.of("COL1", Schema.of(Schema.Type.INT)),
      Schema.Field.of("col:2", Schema.decimalOf(10, 3))
    );
    Schema expectedSchema = Schema.recordOf(
      "expectedSchema",
      Schema.Field.of("col1", Schema.of(Schema.Type.INT)),
      Schema.Field.of("col_2", Schema.decimalOf(10, 3))
    );

    Schema outputSchema = TestingRig.executeAndGetSchema(directives, rows, inputSchema);

    Assert.assertEquals(outputSchema.getFields().size(), expectedSchema.getFields().size());
    for (Schema.Field expectedField : expectedSchema.getFields()) {
      Assert.assertEquals(
        outputSchema.getField(expectedField.getName()).getSchema().getType(), expectedField.getSchema().getType()
      );
    }
  }
}
