/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.utils;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.ErrorRecord;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link RecordConvertor}
 */
public class RecordConvertorTest {

  @Test
  public void testComplexNestedStructureConversion() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "{\n" +
        "    \"name\" : {\n" +
        "        \"fname\" : \"Joltie\",\n" +
        "        \"lname\" : \"Root\"\n" +
        "    },\n" +
        "    \"boolean\" : true,\n" +
        "    \"coordinates\" : [\n" +
        "        12.56,\n" +
        "        45.789\n" +
        "    ],\n" +
        "    \"numbers\" : [\n" +
        "        2," +
        "        2,\n" +
        "        3,\n" +
        "        4,\n" +
        "        5,\n" +
        "        6\n" +
        "    ],\n" +
        "    \"moves\" : [\n" +
        "        { \"a\" : 1, \"b\" : \"X\", \"c\" : 2.8},\n" +
        "        { \"a\" : 2, \"b\" : \"Y\", \"c\" : 232342.8},\n" +
        "        { \"a\" : 3, \"b\" : \"Z\", \"c\" : null},\n" +
        "        { \"a\" : 4, \"b\" : \"U\"}\n" +
        "    ],\n" +
        "    \"integer\" : 1,\n" +
        "    \"double\" : 2.8,\n" +
        "    \"float\" : 45.6,\n" +
        "    \"aliases\" : [\n" +
        "        \"root\",\n" +
        "        \"joltie\",\n" +
        "        \"bunny\"\n" +
        "    ]\n" +
        "}")
    );

    rows = TestingRig.execute(directives, rows);
    Row row = RowHelper.createMergedRow(rows);

    SchemaConverter schemaConvertor = new SchemaConverter();
    RecordConvertor convertor = new RecordConvertor();

    Schema schema = schemaConvertor.toSchema("superrecord", row);
    List<StructuredRecord> outputs = convertor.toStructureRecord(rows, schema);

    Assert.assertEquals(1, outputs.size());
    Assert.assertEquals(6, ((List) outputs.get(0).get("body_numbers")).size());
  }

  @Test
  public void testEmptyString() throws Exception {
    Schema schema = Schema.recordOf("test",
                                    Schema.Field.of("value", Schema.of(Schema.Type.STRING))
                                    );
    String[] directives = new String[] {
      "parse-as-csv body ','",
      "rename body_2 value",
      "drop body"
    };

    List<Row> rows = Arrays.asList(
      new Row().add("body", "a,"),
      new Row().add("body", "b,b")
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<StructuredRecord> results = pipeline.execute(rows, schema);
    List<ErrorRecord> errors = pipeline.errors();
    Assert.assertEquals(2, results.size());
    Assert.assertEquals(0, errors.size());
    Assert.assertEquals("", results.get(0).get("value"));
    Assert.assertEquals("b", results.get(1).get("value"));
  }

  @Test
  public void testNullableEmptyField() throws Exception {
    Schema schema = Schema.recordOf("test",
                                    Schema.Field.of("value", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))
                                    );
    String[] directives = new String[] {
      "parse-as-csv body ','",
      "rename body_2 value",
      "set-type value double",
      "drop body"
    };

    List<Row> rows = Arrays.asList(
      new Row().add("body", "a,1"),
      new Row().add("body", "b,2"),
      new Row().add("body", "c,"),
      new Row().add("body", "d,3"),
      new Row().add("body", "e,")
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<StructuredRecord> results = pipeline.execute(rows, schema);
    List<ErrorRecord> errors = pipeline.errors();
    Assert.assertEquals(5, results.size());
    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void testNullableEmptyArray() throws Exception {
    Schema schema = Schema.recordOf("test", Schema.Field.of("test_id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("values", Schema.nullableOf(Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.INT))))));

    String[] directives = new String[] {};

    List<Row> rows = Arrays.asList(
      new Row().add("test_id", "a").add("values", ImmutableList.of(1)),
      new Row().add("test_id", "b").add("values", null),
      new Row().add("test_id", "c"),
      new Row().add("test_id", "d").add("values", ImmutableList.of()),
      new Row().add("test_id", "e").add("values", ImmutableList.of(1, 2, 3))
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<StructuredRecord> results = pipeline.execute(rows, schema);
    List<ErrorRecord> errors = pipeline.errors();
    Assert.assertEquals(5, results.size());
    Assert.assertEquals(0, errors.size());
  }

  @Test(expected = RecipeException.class)
  public void testNonNullableEmptyField() throws Exception {
    Schema schema = Schema.recordOf("test",
                                    Schema.Field.of("value", Schema.of(Schema.Type.DOUBLE))
    );

    String[] directives = new String[] {
      "parse-as-csv body ','",
      "rename body_2 value",
      "set-type value double",
      "drop body"
    };

    List<Row> rows = Arrays.asList(
      new Row().add("body", "a,1"),
      new Row().add("body", "b,2"),
      new Row().add("body", "c,"),
      new Row().add("body", "d,3"),
      new Row().add("body", "e,")
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<StructuredRecord> results = pipeline.execute(rows, schema);
    List<ErrorRecord> errors = pipeline.errors();
    Assert.assertEquals(5, results.size());
    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void testTypeConversions() throws Exception {
    final Schema schema = Schema.recordOf("record",
                                          Schema.Field.of("body_TimeStamp", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("i2l", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("sh2l", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("s2l", Schema.of(Schema.Type.LONG)),
                                          Schema.Field.of("i2f", Schema.of(Schema.Type.FLOAT)),
                                          Schema.Field.of("s2f", Schema.of(Schema.Type.FLOAT)),
                                          Schema.Field.of("l2f", Schema.of(Schema.Type.FLOAT)),
                                          Schema.Field.of("i2d", Schema.of(Schema.Type.DOUBLE)),
                                          Schema.Field.of("s2d", Schema.of(Schema.Type.DOUBLE)),
                                          Schema.Field.of("l2d", Schema.of(Schema.Type.DOUBLE)),
                                          Schema.Field.of("f2d", Schema.of(Schema.Type.DOUBLE))
    );

    String[] directives = new String[] {
      "parse-as-json body",
      "drop body"
    };

    List<Row> rows = Collections.singletonList(
      new Row("body", "{\"DeviceID\":\"xyz-abc\",\"SeqNo\":1000,\"TimeStamp\":123456,\"LastContact\":123456," +
        "\"IMEI\":\"345rft567hy65\",\"MSISDN\":\"+19999999999\",\"AuthToken\":\"erdfgg34gtded\",\"Position\":" +
        "{\"Lat\":\"20.22\",\"Lon\":\"-130.45\",\"Accuracy\":16,\"Compass\":108.22,\"TimeStamp\":123456}," +
        "\"Battery\":50,\"Alert\":{\"Id\":26,\"Type\":\"SOS\",\"TimeStamp\":123456},\"Steps\":100,\"Calories\":15}")
        .add("i2l", 2)
        .add("sh2l", (short) 1)
        .add("s2l", "2")
        .add("i2f", 1)
        .add("s2f", (short) 2)
        .add("l2f", 1L)
        .add("i2d", 1)
        .add("s2d", (short) 3)
        .add("l2d", 2L)
        .add("f2d", 2.3f)
    );

    RecipePipeline pipeline = TestingRig.execute(directives);
    List<StructuredRecord> results = pipeline.execute(rows, schema);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(123456L, results.get(0).<Long>get("body_TimeStamp").longValue());
    Assert.assertEquals(2L, results.get(0).<Long>get("i2l").longValue());
    Assert.assertEquals(1L, results.get(0).<Long>get("sh2l").longValue());
    Assert.assertEquals(2L, results.get(0).<Long>get("s2l").longValue());
    Assert.assertEquals(1.0f, results.get(0).get("i2f"), 0.0001f);
    Assert.assertEquals(2.0f, results.get(0).get("s2f"), 0.0001f);
    Assert.assertEquals(1.0f, results.get(0).get("l2f"), 0.0001f);
    Assert.assertEquals(1.0d, results.get(0).get("i2d"), 0.0001d);
    Assert.assertEquals(3.0d, results.get(0).get("s2d"), 0.0001d);
    Assert.assertEquals(2.0d, results.get(0).get("l2d"), 0.0001d);
    Assert.assertEquals(2.3d, results.get(0).get("f2d"), 0.0001d);
  }

  @Test
  public void testRowWithLogicalType() throws Exception {
    Row testRow = new Row();
    testRow.add("id", 1);
    testRow.add("name", "abc");
    testRow.add("date", LocalDate.of(2018, 11, 11));
    testRow.add("time", null);
    testRow.add("timestamp", ZonedDateTime.of(2018, 11 , 11 , 11, 11, 11, 0, ZoneId.of("UTC")));

    Schema schema = Schema.recordOf("expectedRecord",
                                    Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("time", Schema.nullableOf(
                                      Schema.of(Schema.LogicalType.TIME_MICROS))),
                                    Schema.Field.of("timestamp", Schema.nullableOf(
                                      Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));


    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("id", 1)
      .set("name", "abc")
      .setDate("date", LocalDate.of(2018, 11, 11))
      .setTime("time", null)
      .setTimestamp("timestamp", ZonedDateTime.of(2018, 11 , 11 , 11, 11, 11, 0, ZoneId.of("UTC"))).build();

    RecordConvertor rc = new RecordConvertor();
    StructuredRecord actual = rc.decodeRecord(testRow, schema);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testArray() throws Exception {
    Row testRow = new Row();
    testRow.add("id", 1);
    testRow.add("name", "abc");
    testRow.add("date", LocalDate.of(2018, 11, 11));
    testRow.add("time", null);
    testRow.add("timestamp", ZonedDateTime.of(2018, 11, 11, 11, 11, 11, 0, ZoneId.of("UTC")));
    testRow.add("array", ImmutableList.of(1, 2, 3));

    Schema schema = Schema.recordOf("expectedRecord",
                                    Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("time", Schema.nullableOf(
                                      Schema.of(Schema.LogicalType.TIME_MICROS))),
                                    Schema.Field.of("timestamp", Schema.nullableOf(
                                      Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                                    Schema.Field.of("array", Schema.nullableOf(
                                      Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.INT))))));

    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("id", 1)
      .set("name", "abc")
      .setDate("date", LocalDate.of(2018, 11, 11))
      .setTime("time", null)
      .setTimestamp("timestamp", ZonedDateTime.of(2018, 11, 11, 11, 11, 11, 0, ZoneId.of("UTC")))
      .set("array", ImmutableList.of(1, 2, 3)).build();

    RecordConvertor rc = new RecordConvertor();
    StructuredRecord actual = rc.decodeRecord(testRow, schema);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testNestedRecordConversion() throws Exception {
    Schema productSchema = Schema.recordOf("product",
                                           Schema.Field.of("id",
                                                           Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                           Schema.Field.of("name",
                                                           Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                           Schema.Field.of("cost",
                                                           Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))
    );
    Schema expectedSchema = Schema.recordOf("recordA",
                                            Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                            Schema.Field.of("product", productSchema)
    );
    Row row = new Row();
    row.add("name_a", "John");
    row.add("product", StructuredRecord.builder(productSchema)
      .set("id", 1)
      .set("name", "Shovel")
      .set("cost", 5.0)
      .build());
    String[] directives = new String[]{
      "rename name_a name",
    };
    RecipePipeline pipeline = TestingRig.execute(directives);
    List<Row> records = Collections.singletonList(row);
    List<StructuredRecord> results = pipeline.execute(records, expectedSchema);
    Assert.assertEquals(expectedSchema, results.get(0).getSchema());
  }

  @Test
  public void testDateTimeConversion() throws RecordConvertorException {
    String fieldName = "field";
    LocalDateTime value = LocalDateTime.now();
    Schema schema = Schema.recordOf("test",
        Schema.Field.of(fieldName, Schema.nullableOf(Schema.of(LogicalType.DATETIME))));
    Row row = new Row();
    row.add(fieldName, value);
    StructuredRecord structuredRecord = new RecordConvertor().decodeRecord(row, schema);
    Assert.assertEquals(value, structuredRecord.getDateTime(fieldName));
  }
}
