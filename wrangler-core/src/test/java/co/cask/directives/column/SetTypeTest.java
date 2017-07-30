/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.column;

import co.cask.cdap.api.common.Bytes;
import co.cask.wrangler.TestingRig;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SetType}
 */
public class SetTypeTest {

  @Test
  public void testToInt() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", new Double(10000.0))
        .add("short_col", new Short("10000")).add("long_col", new Long(10000)).add("float_col", new Float(10000.0))
        .add("bytes_col", new byte[]{0, 0, 39, 16})
    );
    String[] directives = new String[] {
      "set-type str_col int", "set-type int_col i64", "set-type double_col integer",
      "set-type short_col INT", "set-type long_col I64", "set-type float_col Integer", "set-type bytes_col INTEGER"
    };

    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.length(); i ++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof Integer);
      Integer intValue = (Integer) object;
      Assert.assertTrue(intValue.equals(10000));
    }
  }

  @Test
  public void testToShort() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", new Double(10000.0))
        .add("short_col", new Short("10000")).add("long_col", new Long(10000)).add("float_col", new Float(10000.0))
        .add("bytes_col", new byte[]{39, 16})
    );
    String[] directives = new String[] {
      "set-type str_col short", "set-type int_col i32", "set-type double_col Short",
      "set-type short_col I32", "set-type long_col SHORT", "set-type float_col short", "set-type bytes_col short"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.length(); i ++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof Short);
      Short value = (Short) object;
      Assert.assertTrue(value.equals(new Short("10000")));
    }
  }

  @Test
  public void testToLong() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", new Double(10000.0))
        .add("short_col", new Short("10000")).add("long_col", new Long(10000)).add("float_col", new Float(10000.0))
        .add("bytes_col", new byte[]{0, 0, 0, 0 , 0, 0, 39, 16})
    );
    String[] directives = new String[] {
      "set-type str_col long", "set-type int_col Long", "set-type double_col LONG",
      "set-type short_col long", "set-type long_col Long", "set-type float_col LONG", "set-type bytes_col long"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.length(); i ++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof Long);
      Long value = (Long) object;
      Assert.assertTrue(value.equals(new Long(10000)));
    }
  }

  @Test
  public void testToFloat() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("str_col", "10000.00").add("int_col", 10000).add("double_col", new Double(10000.00))
        .add("short_col", new Short("10000")).add("long_col", new Long(10000)).add("float_col", new Float(10000.0))
        .add("bytes_col", new byte[]{70, 28, 64, 0})
    );
    String[] directives = new String[] {
      "set-type str_col float", "set-type int_col Float", "set-type double_col FLOAT",
      "set-type short_col float", "set-type long_col Float", "set-type float_col FLOAT", "set-type bytes_col float"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.length(); i ++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof Float);
      Float value = (Float) object;
      Assert.assertTrue(value.equals(new Float(10000)));
    }
  }

  @Test
  public void testToDouble() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("str_col", "10000.00").add("int_col", 10000).add("double_col", new Double(10000.00))
        .add("short_col", new Short("10000")).add("long_col", new Long(10000)).add("float_col", new Float(10000.0))
        .add("bytes_col", new byte[]{64, -61, -120, 0, 0, 0, 0, 0})
    );
    String[] directives = new String[] {
      "set-type str_col double", "set-type int_col Double", "set-type double_col DOUBLE",
      "set-type short_col double", "set-type long_col Double", "set-type float_col DOUBLE", "set-type bytes_col double"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.length(); i ++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof Double);
      Double value = (Double) object;
      Assert.assertTrue(value.equals(new Double(10000)));
    }
  }

  @Test
  public void testToBoolean() throws Exception {
    List<Row> trueRows = Arrays.asList(
      new Row("str_1", "true").add("str_2", "True").add("str_3", "TRUE")
        .add("int_col", 10000).add("double_col", new Double(10000.00))
        .add("short_col", new Short("10000")).add("long_col", new Long(10000))
        .add("float_col", new Float(10000.0)).add("true_col", true)
    );
    List<Row> falseRows = Arrays.asList(
      new Row("str_1", "false").add("str_2", "False").add("str_3", "FALSE")
        .add("int_col", -10000).add("double_col", new Double(-10000.00))
        .add("short_col", new Short("-10000")).add("long_col", new Long(-10000))
        .add("float_col", new Float(-10000.0)).add("false_col", false)
    );
    String[] directives = new String[] {
      "set-type str_1 bool", "set-type str_2 bool", "set-type str_3 bool", "set-type int_col Bool", "set-type double_col BOOL",
      "set-type short_col boolean", "set-type long_col Boolean", "set-type float_col BOOLEAN", "set-type bytes_col bool"
    };

    List<Row> trueResults = TestingRig.execute(directives, trueRows);
    List<Row> falseResults = TestingRig.execute(directives, falseRows);
    Row trueRow = trueResults.get(0);
    Row falseRow = falseResults.get(0);

    for (int i = 0; i < trueRow.length(); i ++) {
      Object trueObject = trueRow.getValue(i);
      Object falseObject = falseRow.getValue(i);
      Assert.assertTrue(trueObject instanceof Boolean);
      Assert.assertTrue(falseObject instanceof Boolean);
      Boolean trueValue = (Boolean) trueObject;
      Boolean falseValue = (Boolean) falseObject;
      Assert.assertTrue(trueValue);
      Assert.assertFalse(falseValue);
    }
  }

  @Test
  public void testToString() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", new Double(10000))
        .add("short_col", new Short("10000")).add("long_col", new Long(10000)).add("float_col", new Float(10000))
        .add("bytes_col", new byte[]{49, 48, 48, 48, 48})
    );
    String[] directives = new String[] {
      "set-type str_col string", "set-type int_col String", "set-type double_col STRING",
      "set-type short_col string", "set-type long_col String", "set-type float_col STRING", "set-type bytes_col string"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.length(); i ++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof String);
      String value = (String) object;
      if (i == 2 || i == 5) {
        Assert.assertTrue(value.equals("10000.0"));
      }
      else {
        Assert.assertTrue(value.equals("10000"));
      }
    }
  }

  @Test
  public void testToBytes() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", new Double(10000.00))
        .add("short_col", new Short("10000")).add("long_col", new Long(10000)).add("float_col", new Float(10000.0))
        .add("bytes_col", new byte[]{64, -61, -120, 0, 0, 0, 0, 0})
    );
    byte[][] bytesResults = new byte[][] {
      new byte[] {49, 48, 48, 48, 48},
      new byte[] {0, 0, 39, 16},
      new byte[] {64, -61, -120, 0, 0, 0, 0, 0},
      new byte[] {39, 16},
      new byte[] {0, 0, 0, 0, 0, 0, 39, 16},
      new byte[] {70, 28, 64, 0},
      new byte[] {64, -61, -120, 0, 0, 0, 0, 0}
    };
    String[] directives = new String[] {
      "set-type str_col bytes", "set-type int_col Bytes", "set-type double_col BYTES",
      "set-type short_col bytes", "set-type long_col Bytes", "set-type float_col BYTES", "set-type bytes_col bytes"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.length(); i ++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof byte[]);
      byte [] value = (byte[]) object;
      Assert.assertEquals(0, Bytes.compareTo(value, bytesResults[i]));
    }
  }
}
