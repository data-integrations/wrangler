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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.directives.column.Swap;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link Swap}
 */
public class SwapTest {

  @Test
  public void testSwap() throws Exception {
    String[] directives = new String[] {
      "swap a b",
    };

    List<Row> rows = Collections.singletonList(
      new Row("a", 1).add("b", "sample string")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(1, rows.get(0).getValue("b"));
    Assert.assertEquals("sample string", rows.get(0).getValue("a"));
  }

  @Test(expected = RecipeException.class)
  public void testSwapFeildNotFound() throws Exception {
    String[] directives = new String[] {
      "swap a b",
    };

    List<Row> rows = Collections.singletonList(
      new Row("a", 1).add("c", "sample string")
    );

    TestingRig.execute(directives, rows);
  }

  @Test
  public void testGetOutputSchemaForSwappedColumns() throws Exception {
    String[] directives = new String[] {
      "swap :col_A :col_B",
    };
    List<Row> rows = Collections.singletonList(
      new Row("col_A", 1).add("col_B", new BigDecimal("143235.016"))
    );
    Schema inputSchema = Schema.recordOf(
      "inputSchema",
      Schema.Field.of("col_A", Schema.of(Schema.Type.INT)),
      Schema.Field.of("col_B", Schema.decimalOf(10, 3))
    );
    Schema expectedSchema = Schema.recordOf(
      "expectedSchema",
      Schema.Field.of("col_B", Schema.of(Schema.Type.INT)),
      Schema.Field.of("col_A", Schema.decimalOf(10, 3))
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
