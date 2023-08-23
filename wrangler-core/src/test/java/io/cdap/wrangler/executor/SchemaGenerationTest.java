/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.executor;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.utils.RowHelper;
import io.cdap.wrangler.utils.SchemaConverter;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SchemaGenerationTest {
  private static final SchemaConverter schemaConverter = new SchemaConverter();
  @Test
  public void testOutputSchemaGeneration_enabled() throws Exception {
    String[] commands = new String[]{
      "parse-as-csv :body ,",
      "drop :body",
      "set-headers :decimal_col,:all_nulls,:name,:timestamp,:weight",
      "set-type :timestamp double",
    };
    Schema inputSchema = Schema.recordOf(
      "input",
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("decimal_col", Schema.decimalOf(10, 2)),
      Schema.Field.of("null_col", Schema.of(Schema.Type.STRING))
    );
    Schema expectedSchema = Schema.recordOf(
      "expected",
      Schema.Field.of("decimal_col", Schema.nullableOf(Schema.decimalOf(10, 2))), // Precision and scale carried over
      Schema.Field.of("all_nulls", Schema.nullableOf(Schema.of(Schema.Type.STRING))), // Null column not dropped
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("timestamp", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("weight", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );
    List<Row> inputRows = new ArrayList<>();
    inputRows.add(new Row("body", "Larry,1481666448").add("decimal_col", new BigDecimal("3.5")).add("null_col", null));
    inputRows.add(new Row("body", "Barry,,172.3").add("decimal_col", new BigDecimal("23456")).add("null_col", null));

    Schema outputSchema = TestingRig.executeAndGetSchema(commands, inputRows, inputSchema);

    for (Schema.Field field : expectedSchema.getFields()) {
      Assert.assertEquals(field.getName(), outputSchema.getField(field.getName()).getName());
      Assert.assertEquals(field.getSchema(), outputSchema.getField(field.getName()).getSchema());
    }
  }

  @Test
  public void testOutputSchemaGeneration_disabled() throws Exception {
    String[] commands = new String[]{
      "parse-as-csv :body ,",
      "drop :body",
      "set-headers :decimal_col,:all_nulls,:name,:timestamp,:weight",
      "set-type :timestamp double",
    };
    Schema inputSchema = Schema.recordOf(
      "input",
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("decimal_col", Schema.decimalOf(10, 2)),
      Schema.Field.of("null_col", Schema.of(Schema.Type.STRING))
    );
    Schema expectedSchema = Schema.recordOf(
      "expected",
      Schema.Field.of("decimal_col", Schema.nullableOf(Schema.decimalOf(38, 1))), // Default precision, scale inferred
      // all_nulls column dropped
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("timestamp", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("weight", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
      );
    List<Row> inputRows = new ArrayList<>();
    inputRows.add(new Row("body", "Larry,1481666448").add("decimal_col", new BigDecimal("3.5")).add("null_col", null));
    inputRows.add(new Row("body", "Barry,,172.3").add("decimal_col", new BigDecimal("23456")).add("null_col", null));

    List<Row> result = TestingRig.execute(commands, inputRows);

    Schema outputSchema = result.isEmpty() ? null :
      schemaConverter.toSchema("record", RowHelper.createMergedRow(result));

    for (Schema.Field field : expectedSchema.getFields()) {
      Assert.assertEquals(field.getName(), outputSchema.getField(field.getName()).getName());
      Assert.assertEquals(field.getSchema(), outputSchema.getField(field.getName()).getSchema());
    }
  }

  @Test
  public void testOutputSchemaGeneration_columnOrdering() throws Exception {
    Schema inputSchema = Schema.recordOf(
      "input",
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("value", Schema.of(Schema.Type.INT))
    );
    String[] commands = new String[] {
      "parse-as-json :body 1",
      "set-type :value long"
    };
    List<Schema.Field> expectedFields = Arrays.asList(
      Schema.Field.of("value", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("body_A", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("body_B", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("body_C", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))
    );
    List<Row> inputRows = new ArrayList<>();
    inputRows.add(new Row().add("body", "{\"A\":1, \"B\":\"hello\"}").add("value", 10L));
    inputRows.add(new Row().add("body", "{\"C\":1.23, \"A\":1, \"B\":\"world\"}").add("value", 20L));

    Schema outputSchema = TestingRig.executeAndGetSchema(commands, inputRows, inputSchema);

    List<Schema.Field> outputFields = outputSchema.getFields();

    Assert.assertEquals(expectedFields.size(), outputFields.size());
    for (int i = 0; i < expectedFields.size(); i++) {
      Assert.assertEquals(expectedFields.get(i).getName(), outputFields.get(i).getName());
      Assert.assertEquals(expectedFields.get(i).getSchema(), outputFields.get(i).getSchema());
    }
  }
}
