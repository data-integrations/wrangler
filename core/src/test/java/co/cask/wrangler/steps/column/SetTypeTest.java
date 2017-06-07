/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.column;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.executor.TextDirectives;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SetType}
 */
public class SetTypeTest {

  @Test
  public void testSetToString() throws Exception {
    String[] directives = new String[] {
      "set-type str_col string", "set-type int_col string", "set-type double_col string",
      "set-type true_col string", "set-type false_col string"
    };
    List<Record> records = Arrays.asList(
      new Record("str_col", "100").add("int_col", 1).add("double_col", 1.0).
        add("true_col", true).add("false_col", false)
    );
    TextDirectives d = new TextDirectives(directives);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(d, null);
    List<Record> results = pipeline.execute(records);
    Record record = results.get(0);

    for (KeyValue<String, Object> keyValue : record.getFields()) {
      Assert.assertTrue(keyValue.getValue() instanceof String);
    }

    String value1 = (String)record.getValue("str_col");
    String value2 = (String)record.getValue("int_col");
    String value3 = (String)record.getValue("double_col");
    String value4 = (String)record.getValue("true_col");
    String value5 = (String)record.getValue("false_col");

    Assert.assertEquals(value1, "100");
    Assert.assertEquals(value2, "1");
    Assert.assertEquals(value3, "1.0");
    Assert.assertEquals(value4, "true");
    Assert.assertEquals(value5, "false");
  }

  @Test
  public void testSetToInt() throws Exception {
    String[] directives = new String[] {
      "set-type str_col int", "set-type int_col int", "set-type double_col int",
      "set-type true_col int", "set-type false_col int"
    };
    List<Record> records = Arrays.asList(
      new Record("str_col", "100").add("int_col", 1).add("double_col", 1.0).
        add("true_col", true).add("false_col", false)
    );
    TextDirectives d = new TextDirectives(directives);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(d, null);
    List<Record> results = pipeline.execute(records);
    Record record = results.get(0);

    for (KeyValue<String, Object> keyValue : record.getFields()) {
      Assert.assertTrue(keyValue.getValue() instanceof Integer);
    }

    Integer value1 = (Integer)record.getValue("str_col");
    Integer value2 = (Integer)record.getValue("int_col");
    Integer value3 = (Integer)record.getValue("double_col");
    Integer value4 = (Integer)record.getValue("true_col");
    Integer value5 = (Integer)record.getValue("false_col");

    Assert.assertTrue(value1.equals(100));
    Assert.assertTrue(value2.equals(1));
    Assert.assertTrue(value3.equals(1));
    Assert.assertTrue(value4.equals(1));
    Assert.assertTrue(value5.equals(0));
  }

  @Test
  public void testSetToDouble() throws Exception {
    String[] directives = new String[] {
      "set-type str_col double", "set-type int_col double", "set-type double_col double",
      "set-type true_col double", "set-type false_col double"
    };
    List<Record> records = Arrays.asList(
      new Record("str_col", "100").add("int_col", 1).add("double_col", 1.0).
        add("true_col", true).add("false_col", false)
    );
    TextDirectives d = new TextDirectives(directives);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(d, null);
    List<Record> results = pipeline.execute(records);
    Record record = results.get(0);

    for (KeyValue<String, Object> keyValue : record.getFields()) {
      Assert.assertTrue(keyValue.getValue() instanceof Double);
    }

    Double value1 = (Double)record.getValue("str_col");
    Double value2 = (Double)record.getValue("int_col");
    Double value3 = (Double)record.getValue("double_col");
    Double value4 = (Double)record.getValue("true_col");
    Double value5 = (Double)record.getValue("false_col");

    Assert.assertTrue(value1.equals(100.0));
    Assert.assertTrue(value2.equals(1.0));
    Assert.assertTrue(value3.equals(1.0));
    Assert.assertTrue(value4.equals(1.0));
    Assert.assertTrue(value5.equals(0.0));
  }

  @Test
  public void testSetToBoolean() throws Exception {
    String[] directives = new String[] {
      "set-type str_true boolean", "set-type str_false boolean",
      "set-type int_col boolean", "set-type double_col boolean",
      "set-type true_col boolean", "set-type false_col boolean"
    };
    List<Record> records = Arrays.asList(
      new Record("str_true", "true").add("str_false", "false")
        .add("int_col", 1).add("double_col", 0.0).
        add("true_col", true).add("false_col", false)
    );
    TextDirectives d = new TextDirectives(directives);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(d, null);
    List<Record> results = pipeline.execute(records);
    Record record = results.get(0);

    for (KeyValue<String, Object> keyValue : record.getFields()) {
      Assert.assertTrue(keyValue.getValue() instanceof Boolean);
    }

    Boolean value1 = (Boolean)record.getValue("str_true");
    Boolean value2 = (Boolean)record.getValue("str_false");
    Boolean value3 = (Boolean)record.getValue("int_col");
    Boolean value4 = (Boolean)record.getValue("double_col");
    Boolean value5 = (Boolean)record.getValue("true_col");
    Boolean value6 = (Boolean)record.getValue("false_col");

    Assert.assertTrue(value1.equals(true));
    Assert.assertTrue(value2.equals(false));
    Assert.assertTrue(value3.equals(true));
    Assert.assertTrue(value4.equals(false));
    Assert.assertTrue(value5.equals(true));
    Assert.assertTrue(value6.equals(false));
  }
}
