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

import co.cask.TestUtil;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Row;
import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link Json2Schema}
 */
public class Json2SchemaTest {
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
    "set-column body json:parse(body, false)"
  };

  @Test
  public void conversionTest() throws Exception {
    Json2Schema converter = new Json2Schema();
    RecordConvertor recordConvertor = new RecordConvertor();
    JsonParser parser = new JsonParser();
    RecipePipeline executor = TestUtil.execute(directives);
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
}
