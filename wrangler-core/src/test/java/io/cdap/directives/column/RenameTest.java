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
 * Tests {@link Rename}
 */
public class RenameTest {

  @Test (expected = RecipeException.class)
  public void testRenameOfExistingColumns() throws Exception {
    String[] directives = new String[] {
      "rename C2 C4",
    };

    List<Row> rows = Collections.singletonList(
      new Row("C1", "A").add("C2", "B").add("C3", "C").add("C4", "D").add("C5", "E")
    );

    TestingRig.execute(directives, rows);
  }

  @Test (expected = RecipeException.class)
  public void testRenameCaseSensitiveFailure() throws Exception {
    String[] directives = new String[] {
      "rename C1 c4",
    };

    List<Row> rows = Collections.singletonList(
      new Row("C1", "A").add("C2", "B").add("C3", "C").add("C4", "D").add("C5", "E")
    );

    TestingRig.execute(directives, rows);
  }

  @Test
  public void testRenameCaseSensitiveSuccess() throws Exception {
    String[] directives = new String[] {
      "rename C1 c1",
    };

    List<Row> rows = Collections.singletonList(
      new Row("C1", "A").add("C2", "B").add("C3", "C").add("C4", "D").add("C5", "E")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Row row = rows.get(0);
    // here compare the exact string since row is case insensitive
    Assert.assertEquals("c1", row.getFields().get(0).getFirst());
    Assert.assertEquals("A", row.getValue("c1"));
    Assert.assertEquals("B", row.getValue("C2"));
    Assert.assertEquals("C", row.getValue("C3"));
    Assert.assertEquals("D", row.getValue("C4"));
    Assert.assertEquals("E", row.getValue("C5"));
  }

  @Test
  public void testRenameNonExistingCols() throws Exception {
    String[] directives = new String[] {
      "rename body_10 body_3",
      "rename body_3 BODY_3"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "A").add("body_1", "B").add("body_2", "C"),
      new Row("body", "D").add("body_1", "E").add("body_2", "F").add("body_10", "G"),
      new Row("body", "H").add("body_1", "I").add("body_2", "J"),
      new Row("body", "K").add("body_1", "L").add("body_2", "M").add("body_10", "N"));

    List<Row> result = TestingRig.execute(directives, rows);

    // here create new row and compare individual row since row is not immutable during execution
    Assert.assertEquals(4, result.size());
    Assert.assertEquals(new Row("body", "A").add("body_1", "B").add("body_2", "C"), result.get(0));
    Assert.assertEquals(new Row("body", "D").add("body_1", "E").add("body_2", "F").add("BODY_3", "G"), result.get(1));
    Assert.assertEquals(new Row("body", "H").add("body_1", "I").add("body_2", "J"), result.get(2));
    Assert.assertEquals(new Row("body", "K").add("body_1", "L").add("body_2", "M").add("BODY_3", "N"), result.get(3));
  }

  @Test
  public void testGetOutputSchemaForRenamedColumns() throws Exception {
    String[] directives = new String[] {
      "rename :col_B :col_C",
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
      Schema.Field.of("col_A", Schema.of(Schema.Type.INT)),
      Schema.Field.of("col_C", Schema.decimalOf(10, 3))
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
