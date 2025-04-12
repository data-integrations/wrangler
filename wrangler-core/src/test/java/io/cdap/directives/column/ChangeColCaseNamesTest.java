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
import java.util.Arrays;
import java.util.Collections;
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

  @Test
  public void testGetOutputSchemaForCaseChangedCols() throws Exception {
    String[] directives = new String[] {
      "change-column-case lower",
    };
    List<Row> rows = Collections.singletonList(
      new Row("ALL_CAPS", 1).add("MiXeD_CAse", "random").add("all_lower", new BigDecimal("143235.016"))
    );
    Schema inputSchema = Schema.recordOf(
      "inputSchema",
      Schema.Field.of("ALL_CAPS", Schema.of(Schema.Type.INT)),
      Schema.Field.of("MiXeD_CAse", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("all_lower", Schema.decimalOf(10, 3))
    );
    Schema expectedSchema = Schema.recordOf(
      "expectedSchema",
      Schema.Field.of("all_caps", Schema.of(Schema.Type.INT)),
      Schema.Field.of("mixed_case", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("all_lower", Schema.decimalOf(10, 3))
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
