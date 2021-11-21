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

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SchemaConverter}
 */
public class SchemaConverterTest {
  private static final String[] TESTS = new String[] {
    JsonTestData.BASIC,
    JsonTestData.SIMPLE_JSON_OBJECT,
    JsonTestData.ARRAY_OF_OBJECTS,
    JsonTestData.JSON_ARRAY_WITH_OBJECT,
    JsonTestData.COMPLEX_1,
    JsonTestData.ARRAY_OF_NUMBERS,
    JsonTestData.ARRAY_OF_STRING,
    JsonTestData.COMPLEX_2,
    JsonTestData.EMPTY_OBJECT,
    JsonTestData.FB_JSON
  };

  private static final String[] directives = new String[] {
    "set-column body json:Parse(body)"
  };

  @Test
  public void conversionTest() throws Exception {
    SchemaConverter converter = new SchemaConverter();
    RecordConvertor recordConvertor = new RecordConvertor();
    JsonParser parser = new JsonParser();
    RecipePipeline executor = TestingRig.execute(directives);
    for (String test : TESTS) {
      Row row = new Row("body", test);

      List<Row> rows = executor.execute(Lists.newArrayList(row));
      Schema schema = converter.toSchema("myrecord", rows.get(0));
      if (schema.getType() != Schema.Type.RECORD) {
        schema = Schema.recordOf("array", Schema.Field.of("array", schema));
      }
      Assert.assertNotNull(schema);
      List<StructuredRecord> structuredRecords = recordConvertor.toStructureRecord(rows, schema);
      String decode = StructuredRecordStringConverter.toJsonString(structuredRecords.get(0));
      JsonElement originalObject = parser.parse(test);
      JsonElement roundTripObject = parser.parse(decode).getAsJsonObject().get("body");
      Assert.assertEquals(originalObject, roundTripObject);
      Assert.assertTrue(structuredRecords.size() > 0);
    }
  }

  @Test
  public void testJsonPathGeneration() throws Exception {
    JsonPathGenerator paths = new JsonPathGenerator();
    List<String> path = paths.get(JsonTestData.COMPLEX_1);
    Assert.assertEquals(path.size(), 23);
  }

  @Test
  public void testLogicalType() throws Exception {
    Row testRow = new Row();
    testRow.add("id", 1);
    testRow.add("name", "abc");
    testRow.add("date", LocalDate.of(2018, 11, 11));
    testRow.add("time", LocalTime.of(11, 11, 11));
    testRow.add("timestamp", ZonedDateTime.of(2018, 11 , 11 , 11, 11, 11, 0, ZoneId.of("UTC")));
    testRow.add("d", new BigDecimal(new BigInteger("123456"), 5));
    testRow.add("datetime", LocalDateTime.now());

    SchemaConverter schemaConvertor = new SchemaConverter();
    Schema actual = schemaConvertor.toSchema("testRecord", testRow);

    Schema expected = Schema.recordOf("testRecord",
                                      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                      Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                      Schema.Field.of("time", Schema.nullableOf(
                                        Schema.of(Schema.LogicalType.TIME_MICROS))),
                                      Schema.Field.of("timestamp", Schema.nullableOf(
                                        Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                                      Schema.Field.of("d", Schema.nullableOf(Schema.decimalOf(38, 5))),
                                      Schema.Field
                                        .of("datetime", Schema.nullableOf(Schema.of(Schema.LogicalType.DATETIME))));

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testArrayType() throws Exception {
    List<Integer> list = new ArrayList<>();
    list.add(null);
    list.add(null);
    list.add(1);
    list.add(2);

    Row testRow = new Row();
    testRow.add("id", 1);
    testRow.add("name", "abc");
    testRow.add("date", LocalDate.of(2018, 11, 11));
    testRow.add("time", LocalTime.of(11, 11, 11));
    testRow.add("timestamp", ZonedDateTime.of(2018, 11, 11, 11, 11, 11, 0, ZoneId.of("UTC")));
    testRow.add("array", list);

    SchemaConverter schemaConvertor = new SchemaConverter();
    Schema actual = schemaConvertor.toSchema("testRecord", testRow);

    Schema expected = Schema.recordOf("testRecord",
                                      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                      Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                      Schema.Field.of("time", Schema.nullableOf(
                                        Schema.of(Schema.LogicalType.TIME_MICROS))),
                                      Schema.Field.of("timestamp", Schema.nullableOf(
                                        Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                                      Schema.Field.of("array", Schema.nullableOf(
                                        Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.INT))))));
    Assert.assertEquals(expected, actual);
  }


  @Test
  public void testRecordType() throws Exception {
    List<Integer> list = new ArrayList<>();
    list.add(null);
    list.add(null);
    list.add(1);
    list.add(2);

    Row memberRow = new Row();
    memberRow.add("id", 1);
    memberRow.add("name", "abc");
    memberRow.add("date", LocalDate.of(2018, 11, 11));
    memberRow.add("time", LocalTime.of(11, 11, 11));
    memberRow.add("timestamp", ZonedDateTime.of(2018, 11, 11, 11, 11, 11, 0, ZoneId.of("UTC")));
    memberRow.add("array", list);


    String recordTypeName = "struct";
    Row testRow = new Row(recordTypeName, memberRow);


    String topRecord = "testRecord";
    SchemaConverter schemaConvertor = new SchemaConverter();
    Schema actual = schemaConvertor.toSchema(topRecord, testRow);

    List<Schema.Field> fields = Arrays.asList(
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("date", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
      Schema.Field.of("time", Schema.nullableOf(
        Schema.of(Schema.LogicalType.TIME_MICROS))),
      Schema.Field.of("timestamp", Schema.nullableOf(
        Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("array", Schema.nullableOf(
        Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.INT)))))
    );

    Schema namingSchema = Schema.recordOf(fields);
    String newRecordTypeName = recordTypeName + namingSchema.getRecordName();
    Schema memberSchema = Schema.nullableOf(Schema.recordOf(newRecordTypeName, fields));

    Schema expected = Schema.recordOf("testRecord", Schema.Field.of(recordTypeName, memberSchema));
    Assert.assertEquals(expected, actual);
  }

}
