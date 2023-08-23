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
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
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
  @Test
  public void testGetOutputSchemaForReplacedColumnNames() throws Exception {
    String[] directives = new String[] {
      "columns-replace s/^data_//g",
    };
    List<Row> rows = Collections.singletonList(
      new Row("data_a", 1).add("data_data_confuse", "ABC").add("no_data", null).add("random", new BigDecimal("12.44"))
    );
    Schema inputSchema = Schema.recordOf(
      "inputSchema",
      Schema.Field.of("data_a", Schema.of(Schema.Type.INT)),
      Schema.Field.of("data_data_confuse", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("no_data", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("random", Schema.decimalOf(10, 3))
    );
    Schema expectedSchema = Schema.recordOf(
      "expectedSchema",
      Schema.Field.of("a", Schema.of(Schema.Type.INT)),
      Schema.Field.of("data_confuse", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("no_data", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("random", Schema.decimalOf(10, 3))
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
