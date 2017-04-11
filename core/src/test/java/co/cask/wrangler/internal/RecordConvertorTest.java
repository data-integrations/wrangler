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
import co.cask.wrangler.utils.Json2Schema;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
import co.cask.wrangler.utils.RecordConvertor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
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

    List<Record> records = Arrays.asList(
      new Record("body", "{\n" +
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

    records = PipelineTest.execute(directives, records);
    Record record = createUberRecord(records);

    Json2Schema json2Schema = new Json2Schema();
    RecordConvertor convertor = new RecordConvertor();

    Schema schema = json2Schema.toSchema("superrecord", record);
    List<StructuredRecord> outputs = convertor.toStructureRecord(records, schema);

    Assert.assertEquals(1, outputs.size());
    Assert.assertEquals(6, ((List)outputs.get(0).get("body_numbers")).size());
  }

  private static Record createUberRecord(List<Record> records) {
    Record uber = new Record();
    for (Record record : records) {
      for (int i = 0; i < record.length(); ++i) {
        Object o = record.getValue(i);
        uber.addOrSet(record.getColumn(i), null);
        if (o != null) {
          uber.addOrSet(record.getColumn(i), o);
        }
      }
    }
    return uber;
  }
}
