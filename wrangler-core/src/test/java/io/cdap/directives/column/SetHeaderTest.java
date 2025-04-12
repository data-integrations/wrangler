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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link SetHeader}
 */
public class SetHeaderTest {

  @Test(expected = RecipeException.class)
  public void testEmptySetColumnsDirectiveAtStart() throws Exception {
    String[] directives = {
      "set-header ,A,B"
    };
    TestingRig.execute(directives, new ArrayList<>());
  }

  @Test(expected = RecipeException.class)
  public void testEmptySetColumnsDirectiveInMiddle() throws Exception {
    String[] directives = {
      "set-header A,B, ,D"
    };
    TestingRig.execute(directives, new ArrayList<>());
  }

  @Test(expected = RecipeException.class)
  public void testEmptySetColumnsDirectiveAtEnd1() throws Exception {
    String[] directives = {
      "set-header A,B,D,"
    };
    TestingRig.execute(directives, new ArrayList<>());
    Assert.assertTrue(true);
  }

  @Test(expected = RecipeException.class)
  public void testEmptySetColumnsDirectiveAtEnd2() throws Exception {
    String[] directives = {
      "set-header A,B,D,,"
    };
    TestingRig.execute(directives, new ArrayList<>());
    Assert.assertTrue(true);
  }

  @Test
  public void testGetOutputSchemaAfterSettingHeader() throws Exception {
    String[] directives = new String[] {
      "set-headers :new_A ,:new_B",
    };
    List<Row> rows = Collections.singletonList(
      new Row("col_A", 1).add("col_B", new BigDecimal("123.456")).add("col_c", "hello world")
    );
    Schema inputSchema = Schema.recordOf(
      "inputSchema",
      Schema.Field.of("col_A", Schema.of(Schema.Type.INT)),
      Schema.Field.of("col_B", Schema.decimalOf(10, 3)),
      Schema.Field.of("col_c", Schema.of(Schema.Type.STRING))
    );
    Schema expectedSchema = Schema.recordOf(
      "expectedSchema",
      Schema.Field.of("new_A", Schema.of(Schema.Type.INT)),
      Schema.Field.of("new_B", Schema.decimalOf(10, 3)),
      Schema.Field.of("col_c", Schema.of(Schema.Type.STRING))
    );

    Schema outputSchema = TestingRig.executeAndGetSchema(directives, rows, inputSchema);

    Assert.assertEquals(expectedSchema.getFields().size(), outputSchema.getFields().size());
    for (Schema.Field expectedField : expectedSchema.getFields()) {
      Assert.assertEquals(
        outputSchema.getField(expectedField.getName()).getSchema().getType(), expectedField.getSchema().getType()
      );
    }
  }
}
