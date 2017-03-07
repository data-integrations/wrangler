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

package co.cask.wrangler.internal;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link RecordConvertor}
 */
public class RecordConvertorTest {

  @Test
  public void testWithFile() throws Exception {
    Path path = Paths.get("/Users/nitin/Downloads/CDAP_Package/json_array_surveyfacts.json");
    byte[] data = Files.readAllBytes(path);

    String[] directives = new String[] {
      "parse-as-json body",
      "drop body"
    };

    List<Record> records = Arrays.asList(
      new Record("body", new String(data))
    );
    RecordConvertor convertor = new RecordConvertor();
    records = PipelineTest.execute(directives, records);
    Schema schema = convertor.toSchema("record", records);
    Assert.assertTrue(true);
  }

  @Test
  public void testComplexStructureConversion() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "{\n" +
        "    \"name\" : {\n" +
        "        \"fname\" : \"Joltie\",\n" +
        "        \"lname\" : \"Root\",\n" +
        "        \"mname\" : null\n" +
        "    },\n" +
        "    \"coordinates\" : [\n" +
        "        12.56,\n" +
        "        45.789\n" +
        "    ],\n" +
        "    \"numbers\" : [\n" +
        "        1,\n" +
        "        2,\n" +
        "        3,\n" +
        "        null,\n" +
        "        4,\n" +
        "        5,\n" +
        "        6,\n" +
        "        null\n" +
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
        "        \"bunny\",\n" +
        "        null\n" +
        "    ]\n" +
        "}")
    );

    RecordConvertor convertor = new RecordConvertor();
    records = PipelineTest.execute(directives, records);
    Schema schema = convertor.toSchema("record", records);
    List<StructuredRecord> outputs = convertor.toStructureRecord(records, schema);
    Assert.assertEquals(1, outputs.size());
    Assert.assertEquals(4, ((List)outputs.get(0).get("body_moves")).size());
  }

  @Test
  public void testSchemaGeneration() throws Exception {
    Map<String, String> values = new HashMap<>(); values.put("foo", "1");
    JSONArray array1 = new JSONArray(); array1.put("a"); array1.put("b");
    JSONArray array2 = new JSONArray(); array2.put(1); array2.put(2);
    JSONArray array3 = new JSONArray(); array3.put(1.9); array3.put(4.5);
    JSONArray array4 = new JSONArray();
    JSONObject o1 = new JSONObject();
    o1.put("name", "test");
    o1.put("age", 34);
    o1.put("salary", 5.6);
    o1.put("address", JSONObject.NULL);
    JSONObject o2 = new JSONObject();
    o2.put("name", "joltie");
    o2.put("age", 22);
    o2.put("salary", JSONObject.NULL);
    o2.put("address", "5787 Mars Ave, Mars, 34242");
    array4.put(o1);
    array4.put(o2);

    RecordConvertor convertor = new RecordConvertor();
    Record record = new Record("values", values).add("int", 1).add("string", "2").add("double", 2.3).add("array1", array1)
      .add("array2", array2).add("array3", array3).add("arrayobject", array4);
    Schema schema = convertor.toSchema("record", Arrays.asList(record));
    Assert.assertNotNull(schema);
  }
}
