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

package co.cask.wrangler.utils;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.TestUtil;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.SimpleTextParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link RecordConvertor}
 */
public class RowConvertorTest {

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

    rows = TestUtil.execute(directives, rows);
    Row row = createUberRecord(rows);

    Json2Schema json2Schema = new Json2Schema();
    RecordConvertor convertor = new RecordConvertor();

    Schema schema = json2Schema.toSchema("superrecord", row);
    List<StructuredRecord> outputs = convertor.toStructureRecord(rows, schema);

    Assert.assertEquals(1, outputs.size());
    Assert.assertEquals(6, ((List)outputs.get(0).get("body_numbers")).size());
  }

  private static Row createUberRecord(List<Row> rows) {
    Row uber = new Row();
    for (Row row : rows) {
      for (int i = 0; i < row.length(); ++i) {
        Object o = row.getValue(i);
        uber.addOrSet(row.getColumn(i), null);
        if (o != null) {
          uber.addOrSet(row.getColumn(i), o);
        }
      }
    }
    return uber;
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

    List<Row> rows = Arrays.asList(
      new Row("body", "{\"DeviceID\":\"xyz-abc\",\"SeqNo\":1000,\"TimeStamp\":123456,\"LastContact\":123456," +
        "\"IMEI\":\"345rft567hy65\",\"MSISDN\":\"+19999999999\",\"AuthToken\":\"erdfgg34gtded\",\"Position\":" +
        "{\"Lat\":\"20.22\",\"Lon\":\"-130.45\",\"Accuracy\":16,\"Compass\":108.22,\"TimeStamp\":123456}," +
        "\"Battery\":50,\"Alert\":{\"Id\":26,\"Type\":\"SOS\",\"TimeStamp\":123456},\"Steps\":100,\"Calories\":15}")
        .add("i2l", new Integer(2))
        .add("sh2l", new Short((short)1))
        .add("s2l", new String("2"))
        .add("i2f", new Integer(1))
        .add("s2f", new Short((short)2))
        .add("l2f", new Long(1))
        .add("i2d", new Integer(1))
        .add("s2d", new Short((short)3))
        .add("l2d", new Long(2))
        .add("f2d", new Float(2.3))

    );

    SimpleTextParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    List<StructuredRecord> results = pipeline.execute(rows, schema);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(123456L, results.get(0).get("body_TimeStamp"));
    Assert.assertEquals(2L, results.get(0).get("i2l"));
    Assert.assertEquals(1L, results.get(0).get("sh2l"));
    Assert.assertEquals(2L, results.get(0).get("s2l"));
    Assert.assertEquals(1.0f, results.get(0).get("i2f"));
    Assert.assertEquals(2.0f, results.get(0).get("s2f"));
    Assert.assertEquals(1.0f, results.get(0).get("l2f"));
    Assert.assertEquals(1.0, results.get(0).get("i2d"));
    Assert.assertEquals(3.0, results.get(0).get("s2d"));
    Assert.assertEquals(2.0, results.get(0).get("l2d"));
    Assert.assertEquals(2.3, (Double)results.get(0).get("f2d"), 0.01);
  }
}
