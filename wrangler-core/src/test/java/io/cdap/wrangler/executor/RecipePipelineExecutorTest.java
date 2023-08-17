/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.TestingPipelineContext;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.schema.TransientStoreKeys;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link RecipePipelineExecutor}.
 */
public class RecipePipelineExecutorTest {

  @Test
  public void testPipeline() throws Exception {

    String[] commands = new String[] {
      "parse-as-csv __col ,",
      "drop __col",
      "set columns a,b,c,d,e,f,g",
      "rename a first",
      "drop b"
    };
    // Output schema
    Schema schema = Schema.recordOf(
      "output",
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("c", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("d", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("e", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("f", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("g", Schema.of(Schema.Type.STRING))
    );

    RecipePipeline pipeline = TestingRig.execute(commands);

    Row row = new Row("__col", "a,b,c,d,e,f,1.0");
    StructuredRecord record = (StructuredRecord) pipeline.execute(Collections.singletonList(row), schema).get(0);

    // Validate the {@link StructuredRecord}
    Assert.assertEquals("a", record.get("first"));
    Assert.assertEquals("c", record.get("c"));
    Assert.assertEquals("d", record.get("d"));
    Assert.assertEquals("e", record.get("e"));
    Assert.assertEquals("f", record.get("f"));
    Assert.assertEquals("1.0", record.get("g"));
  }


  @Test
  public void testPipelineWithMoreSimpleTypes() throws Exception {

    String[] commands = new String[] {
      "parse-as-csv __col ,",
      "drop __col",
      "set columns first,last,email,timestamp,weight"
    };
    // Output schema
    Schema schema = Schema.recordOf(
      "output",
      Schema.Field.of("first", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("last", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("email", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("weight", Schema.of(Schema.Type.FLOAT))
    );

    RecipePipeline pipeline = TestingRig.execute(commands);
    Row row = new Row("__col", "Larry,Perez,lperezqt@umn.edu,1481666448,186.66");
    StructuredRecord record = (StructuredRecord) pipeline.execute(Collections.singletonList(row), schema).get(0);

    // Validate the {@link StructuredRecord}
    Assert.assertEquals("Larry", record.get("first"));
    Assert.assertEquals("Perez", record.get("last"));
    Assert.assertEquals("lperezqt@umn.edu", record.get("email"));
    Assert.assertEquals(1481666448L, record.<Long>get("timestamp").longValue());
    Assert.assertEquals(186.66f, record.get("weight"), 0.0001f);
  }

  @Test
  public void testOutputSchemaGeneration() throws Exception {
    String[] commands = new String[]{
      "parse-as-csv :body ,",
      "drop :body",
      "set-headers :decimal_col,:name,:timestamp,:weight,:date",
      "set-type :timestamp double",
    };
    Schema inputSchema = Schema.recordOf(
      "input",
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("decimal_col", Schema.decimalOf(10, 2))
    );
    Schema expectedSchema = Schema.recordOf(
      "expected",
      Schema.Field.of("decimal_col", Schema.nullableOf(Schema.decimalOf(10, 2))),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("timestamp", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("weight", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );
    List<Row> inputRows = new ArrayList<>();
    inputRows.add(new Row("body", "Larry,1481666448,01/01/2000").add("decimal_col", new BigDecimal("123.45")));
    inputRows.add(new Row("body", "Barry,,172.3,05/01/2000").add("decimal_col", new BigDecimal("234235456.0000")));
    ExecutorContext context = new TestingPipelineContext();
    context.getTransientStore().set(
      TransientVariableScope.GLOBAL, TransientStoreKeys.INPUT_SCHEMA, inputSchema);

    TestingRig.execute(commands, inputRows, context);
    Schema outputSchema = context.getTransientStore().get(TransientStoreKeys.OUTPUT_SCHEMA);

    for (Schema.Field field : expectedSchema.getFields()) {
      Assert.assertEquals(field.getName(), outputSchema.getField(field.getName()).getName());
      Assert.assertEquals(field.getSchema(), outputSchema.getField(field.getName()).getSchema());
    }
  }

  @Test
  public void testOutputSchemaGeneration_doesNotDropNullColumn() throws Exception {
    Schema inputSchema = Schema.recordOf(
      "input",
      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("null_col", Schema.of(Schema.Type.STRING))
    );
    String[] commands = new String[]{"set-type :id int"};
    Schema expectedSchema = Schema.recordOf(
      "expected",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("null_col", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );
    Row row = new Row();
    row.add("id", "123");
    row.add("null_col", null);

    Schema outputSchema = TestingRig.executeAndGetSchema(commands, Collections.singletonList(row), inputSchema);

    Assert.assertEquals(expectedSchema.getField("null_col").getSchema(), outputSchema.getField("null_col").getSchema());
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
    Row row1 = new Row().add("body", "{\"A\":1, \"B\":\"hello\"}").add("value", 10L);
    Row row2 = new Row().add("body", "{\"C\":1.23, \"A\":1, \"B\":\"world\"}").add("value", 20L);
    ExecutorContext context = new TestingPipelineContext();
    context.getTransientStore().set(TransientVariableScope.GLOBAL, TransientStoreKeys.INPUT_SCHEMA, inputSchema);

    TestingRig.execute(commands, Arrays.asList(row1, row2), context);
    Schema outputSchema = context.getTransientStore().get(TransientStoreKeys.OUTPUT_SCHEMA);
    List<Schema.Field> outputFields = outputSchema.getFields();

    Assert.assertEquals(expectedFields.size(), outputFields.size());
    for (int i = 0; i < expectedFields.size(); i++) {
      Assert.assertEquals(expectedFields.get(i).getName(), outputFields.get(i).getName());
      Assert.assertEquals(expectedFields.get(i).getSchema(), outputFields.get(i).getSchema());
    }
  }
}
