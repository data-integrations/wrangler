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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link SetType}
 */
public class SetTypeTest {

  @Test
  public void testToInt() throws Exception {
    List<Row> rows = Collections.singletonList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", 10000.0d)
        .add("short_col", new Short("10000")).add("long_col", 10000L).add("float_col", 10000.0f)
        .add("bytes_col", new byte[]{0, 0, 39, 16}).add("decimal_col", new BigDecimal("10000"))
    );
    String[] directives = new String[] {
      "set-type str_col int", "set-type int_col i64", "set-type double_col integer", "set-type short_col INT",
      "set-type long_col I64", "set-type float_col Integer", "set-type bytes_col INTEGER",
      "set-type decimal_col INTEGER"
    };

    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.width(); i++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof Integer);
      Integer intValue = (Integer) object;
      Assert.assertEquals(10000, (int) intValue);
    }
  }

  @Test
  public void testToShort() throws Exception {
    List<Row> rows = Collections.singletonList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", 10000.0d)
        .add("short_col", new Short("10000")).add("long_col", 10000L).add("float_col", 10000.0f)
        .add("bytes_col", new byte[]{39, 16}).add("decimal_col", new BigDecimal("10000"))
    );
    String[] directives = new String[] {
      "set-type str_col short", "set-type int_col i32", "set-type double_col Short", "set-type short_col I32",
      "set-type long_col SHORT", "set-type float_col short", "set-type bytes_col short", "set-type decimal_col SHORT"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.width(); i++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof Short);
      Short value = (Short) object;
      Assert.assertTrue(value.equals(new Short("10000")));
    }
  }

  @Test
  public void testToLong() throws Exception {
    List<Row> rows = Collections.singletonList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", 10000.0d)
        .add("short_col", new Short("10000")).add("long_col", 10000L).add("float_col", 10000.0f)
        .add("bytes_col", new byte[]{0, 0, 0, 0 , 0, 0, 39, 16}).add("decimal_col", new BigDecimal("10000"))
    );
    String[] directives = new String[] {
      "set-type str_col long", "set-type int_col Long", "set-type double_col LONG", "set-type short_col long",
      "set-type long_col Long", "set-type float_col LONG", "set-type bytes_col long", "set-type decimal_col long"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.width(); i++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof Long);
      Long value = (Long) object;
      Assert.assertEquals(10000L, (long) value);
    }
  }

  @Test
  public void testToFloat() throws Exception {
    List<Row> rows = Collections.singletonList(
      new Row("str_col", "10000.00").add("int_col", 10000).add("double_col", 10000.00d)
        .add("short_col", new Short("10000")).add("long_col", 10000L).add("float_col", 10000.0f)
        .add("bytes_col", new byte[]{70, 28, 64, 0}).add("decimal_col", new BigDecimal("10000"))
    );
    String[] directives = new String[] {
      "set-type str_col float", "set-type int_col Float", "set-type double_col FLOAT", "set-type short_col float",
      "set-type long_col Float", "set-type float_col FLOAT", "set-type bytes_col float", "set-type decimal_col Float"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.width(); i++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof Float);
      Float value = (Float) object;
      Assert.assertTrue(value.equals(new Float(10000)));
    }
  }

  @Test
  public void testToDouble() throws Exception {
    List<Row> rows = Collections.singletonList(
      new Row("str_col", "10000.00").add("int_col", 10000).add("double_col", 10000.00d)
        .add("short_col", new Short("10000")).add("long_col", 10000L).add("float_col", 10000.0f)
        .add("bytes_col", new byte[]{64, -61, -120, 0, 0, 0, 0, 0}).add("decimal_col", new BigDecimal("10000"))
    );
    String[] directives = new String[] {
      "set-type str_col double", "set-type int_col Double", "set-type double_col DOUBLE", "set-type short_col double",
      "set-type long_col Double", "set-type float_col DOUBLE", "set-type bytes_col double",
      "set-type decimal_col Double"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.width(); i++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof Double);
      Double value = (Double) object;
      Assert.assertTrue(value.equals(new Double(10000)));
    }
  }

  @Test
  public void testToDecimal() throws Exception {
    List<Row> rows = Collections.singletonList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", 10000.00d)
        .add("short_col", new Short("10000")).add("long_col", 10000L).add("float_col", 10000.0f)
        .add("bytes_col", new byte[] {0, 0, 0, 0, 39, 16}).add("decimal_col", new BigDecimal("10000"))
    );
    String[] directives = new String[] {
      "set-type str_col decimal", "set-type int_col Decimal", "set-type double_col DECIMAL",
      "set-type short_col DECIMAL", "set-type long_col Decimal", "set-type float_col DECIMAL",
      "set-type bytes_col decimal", "set-type decimal_col decimal"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);
    for (int i = 0; i < row.width(); i++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof BigDecimal);
      BigDecimal value = (BigDecimal) object;
      if (i == 2 || i == 5) {
        Assert.assertTrue(new BigDecimal("10000.0").equals(value));
      } else {
        Assert.assertTrue(new BigDecimal("10000").equals(value));
      }
    }
  }

  @Test
  public void testToDecimalWithRound() throws Exception {
    List<Row> rows = Collections.singletonList(new Row("scale_1", "122.5").add("scale_3", "456.789"));
    String[] directives = new String[] {"set-type scale_1 decimal 0", "set-type scale_3 decimal 0 'FLOOR'"};
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    Assert.assertTrue(row.getValue(0) instanceof BigDecimal);
    Assert.assertEquals(row.getValue(0), new BigDecimal("122"));

    Assert.assertTrue(row.getValue(1) instanceof BigDecimal);
    Assert.assertEquals(row.getValue(1), new BigDecimal("456"));
  }

  @Test
  public void testToDecimalNegativeScale() throws Exception {
    List<Row> rows = Collections.singletonList(new Row("scale_2", "125.45"));
    String[] directives = new String[] {"set-type scale_2 decimal -1 'HALF_UP'"};
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    Assert.assertTrue(row.getValue(0) instanceof BigDecimal);
    Assert.assertEquals(row.getValue(0), new BigDecimal("1.3E+2"));
  }

  @Test(expected = RecipeException.class)
  public void testToDecimalRoundingRequired() throws Exception {
    List<Row> rows = Collections.singletonList(new Row("scale_2", "123.45"));
    String[] directives = new String[] {"set-type scale_2 decimal 1 'UNNECESSARY'"};
    TestingRig.execute(directives, rows);
  }

  @Test(expected = RecipeException.class)
  public void testToDecimalInvalidRoundingMode() throws Exception {
    List<Row> rows = Collections.singletonList(new Row("scale_2", "123.45"));
    String[] directives = new String[] {"set-type scale_2 decimal 3 'RANDOM'"};
    TestingRig.execute(directives, rows);
  }

  @Test
  public void testToDecimalWithScalePrecisionAndRoundingMode() throws Exception {
    List<Row> rows = Collections.singletonList(new Row("scale_1_precision_4", "122.5")
        .add("scale_3_precision_6", "456.789"));
    String[] directives = new String[] {"set-type :scale_1_precision_4 decimal 0 'FLOOR' prop:{precision=3}",
        "set-type :scale_3_precision_6 decimal 0 prop:{precision=5}"};
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    Assert.assertTrue(row.getValue(0) instanceof BigDecimal);
    Assert.assertEquals(row.getValue(0), new BigDecimal("122"));

    Assert.assertTrue(row.getValue(1) instanceof BigDecimal);
    Assert.assertEquals(row.getValue(1), new BigDecimal("457"));
  }

  @Test
  public void testToDecimalWithPrecision() throws Exception {
    List<Row> rows = Collections.singletonList(new Row("scale_1_precision_4", "122.5"));
    String[] directives = new String[] {"set-type :scale_1_precision_4 decimal 'FLOOR' prop:{precision=3}"};
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    Assert.assertTrue(row.getValue(0) instanceof BigDecimal);
    Assert.assertEquals(row.getValue(0), new BigDecimal("122"));

  }

  @Test(expected = RecipeException.class)
  public void testToDecimalWithInvalidPrecision() throws Exception {
    List<Row> rows = Collections.singletonList(new Row("scale_1_precision_4", "122.5"));
    String[] directives = new String[] {"set-type :scale_1_precision_4 decimal 0 'FLOOR' prop:{precision=-1}"};
    TestingRig.execute(directives, rows);
  }

  @Test
  public void testToDecimalScaleIsNull() throws Exception {
    List<Row> rows = Collections.singletonList(new Row("scale_2", "125.45"));
    String[] directives = new String[] {"set-type scale_2 decimal"};
    Schema inputSchema = Schema.recordOf(
        "inputSchema",
        Schema.Field.of("scale_2", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))
    );

    Schema expectedSchema = Schema.recordOf(
        "expectedSchema",
        Schema.Field.of("scale_2", Schema.decimalOf(77, 38))
    );

    List<Row> results = TestingRig.execute(directives, rows);
    Schema outputSchema = TestingRig.executeAndGetSchema(directives, rows, inputSchema);
    Row row = results.get(0);
    Schema.Field outputSchemaField = expectedSchema.getFields().get(0);

    Assert.assertTrue(row.getValue(0) instanceof BigDecimal);
    Assert.assertEquals(row.getValue(0), new BigDecimal("125.45"));

    Assert.assertEquals(outputSchemaField.getSchema().getType(),
        outputSchema.getField(outputSchemaField.getName()).getSchema().getNonNullable().getType());
    Assert.assertEquals(outputSchemaField.getSchema().getPrecision(),
        outputSchema.getField(outputSchemaField.getName()).getSchema().getNonNullable().getPrecision());
    Assert.assertEquals(outputSchemaField.getSchema().getScale(),
        outputSchema.getField(outputSchemaField.getName()).getSchema().getNonNullable().getScale());
  }

  @Test
  public void testToBoolean() throws Exception {
    List<Row> trueRows = Collections.singletonList(
      new Row("str_1", "true").add("str_2", "True").add("str_3", "TRUE")
        .add("int_col", 10000).add("double_col", 10000.00d)
        .add("short_col", new Short("10000")).add("long_col", 10000L)
        .add("float_col", 10000.0f).add("decimal_col", new BigDecimal("10000"))
        .add("true_col", true)
    );
    List<Row> falseRows = Collections.singletonList(
      new Row("str_1", "false").add("str_2", "False").add("str_3", "FALSE")
        .add("int_col", -10000).add("double_col", -10000.00d)
        .add("short_col", new Short("-10000")).add("long_col", -10000L)
        .add("float_col", -10000.0f).add("decimal_col", new BigDecimal("10000").negate())
        .add("false_col", false)
    );
    String[] directives = new String[] {
      "set-type str_1 bool", "set-type str_2 bool", "set-type str_3 bool", "set-type int_col Bool",
      "set-type double_col BOOL", "set-type short_col boolean", "set-type long_col Boolean",
      "set-type float_col BOOLEAN", "set-type bytes_col bool", "set-type decimal_col Boolean"
    };

    List<Row> trueResults = TestingRig.execute(directives, trueRows);
    List<Row> falseResults = TestingRig.execute(directives, falseRows);
    Row trueRow = trueResults.get(0);
    Row falseRow = falseResults.get(0);

    for (int i = 0; i < trueRow.width(); i++) {
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
    LocalDateTime now = LocalDateTime.now();
    List<Row> rows = Collections.singletonList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", 10000d)
        .add("short_col", new Short("10000")).add("long_col", 10000L).add("float_col", 10000f)
        .add("bytes_col", new byte[]{49, 48, 48, 48, 48}).add("decimal_col", new BigDecimal("10000"))
        .add ("datetime_col", now)
    );
    String[] directives = new String[] {
      "set-type str_col string", "set-type int_col String", "set-type double_col STRING", "set-type short_col string",
      "set-type long_col String", "set-type float_col STRING", "set-type bytes_col string",
      "set-type decimal_col STRING", "set-type datetime_col STRING"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.width(); i++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof String);
      String value = (String) object;
      if (i == 2 || i == 5) {
        Assert.assertEquals("10000.0", value);
      } else if (i == 8) {
        Assert.assertEquals(now.toString(), value);
      } else {
        Assert.assertEquals("10000", value);
      }
    }
  }

  @Test
  public void testTimestampToString() throws Exception {
    ZonedDateTime zdt = ZonedDateTime.now();
    List<Row> rows = Collections.singletonList(
        new Row("timestamp_col", zdt)
    );
    String[] directives = new String[] {
        "set-type timestamp_col string"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);
    Object object = row.getValue(0);
    Assert.assertTrue(object instanceof String);
    Assert.assertEquals(zdt.toString(), object);
  }

  @Test
  public void testToBytes() throws Exception {
    List<Row> rows = Collections.singletonList(
      new Row("str_col", "10000").add("int_col", 10000).add("double_col", 10000.00d)
        .add("short_col", new Short("10000")).add("long_col", 10000L).add("float_col", 10000.0f)
        .add("bytes_col", new byte[] {64, -61, -120, 0, 0, 0, 0, 0}).add("decimal_col", new BigDecimal("10000"))
    );
    byte[][] bytesResults = new byte[][] {
      new byte[] {49, 48, 48, 48, 48},
      new byte[] {0, 0, 39, 16},
      new byte[] {64, -61, -120, 0, 0, 0, 0, 0},
      new byte[] {39, 16},
      new byte[] {0, 0, 0, 0, 0, 0, 39, 16},
      new byte[] {70, 28, 64, 0},
      new byte[] {64, -61, -120, 0, 0, 0, 0, 0},
      new byte[] {0, 0, 0, 0, 39, 16}
    };
    String[] directives = new String[] {
      "set-type str_col bytes", "set-type int_col Bytes", "set-type double_col BYTES", "set-type short_col bytes",
      "set-type long_col Bytes", "set-type float_col BYTES", "set-type bytes_col bytes", "set-type decimal_col bytes"
    };
    List<Row> results = TestingRig.execute(directives, rows);
    Row row = results.get(0);

    for (int i = 0; i < row.width(); i++) {
      Object object = row.getValue(i);
      Assert.assertTrue(object instanceof byte[]);
      byte [] value = (byte[]) object;
      Assert.assertEquals(0, Bytes.compareTo(value, bytesResults[i]));
    }
  }

  @Test
  public void testGetOutputSchemaForTypeChangedColumn() throws Exception {
    String[] directives = new String[] {
      "set-type :A I64",
      "set-type :B shoRT",
      "set-type :C decimal 5 HALF_UP",
      "set-type :D bytes",
      "set-type :E string",
      "set-type :F BOOLEAN",
      "set-type :G double"
    };
    List<Row> rows = Collections.singletonList(
      new Row("A", "1234").add("B", "1").add("C", "143235.016")
        .add("D", "random").add("E", 123).add("F", "true").add("G", 12L)
    );
    Schema inputSchema = Schema.recordOf(
        "inputSchema",
        Schema.Field.of("A", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("B", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("C", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("D", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("E", Schema.nullableOf(Schema.of(Schema.Type.INT))),
        Schema.Field.of("F", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("G", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );
    Schema expectedSchema = Schema.recordOf(
      "expectedSchema",
      Schema.Field.of("A", Schema.of(Schema.Type.INT)),
      Schema.Field.of("B", Schema.of(Schema.Type.INT)),
      Schema.Field.of("C", Schema.decimalOf(38, 5)),
      Schema.Field.of("D", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("E", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("F", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("G", Schema.of(Schema.Type.DOUBLE))
    );

    Schema outputSchema = TestingRig.executeAndGetSchema(directives, rows, inputSchema);

    Assert.assertEquals(outputSchema.getFields().size(), expectedSchema.getFields().size());
    for (Schema.Field expectedField : expectedSchema.getFields()) {
      Assert.assertEquals(expectedField.getSchema().getType(),
                          outputSchema.getField(expectedField.getName()).getSchema().getNonNullable().getType());
    }
  }
}
