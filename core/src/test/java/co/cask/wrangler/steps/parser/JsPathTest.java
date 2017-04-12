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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link JsPath}
 */
public class JsPathTest {
  @Test
  public void testJSONFunctions() throws Exception {
    List<Record> records = Arrays.asList(
      new Record("body", "{\n" +
        "    \"name\" : {\n" +
        "        \"Fname\" : \"Joltie\",\n" +
        "        \"Lname\" : \"Root\",\n" +
        "        \"mname\" : null\n" +
        "    },\n" +
        "    \"coordinates\" : [\n" +
        "        12.56,\n" +
        "        45.789\n" +
        "    ],\n" +
        "    \"numbers\" : [\n" +
        "        1,\n" +
        "        2.1,\n" +
        "        3,\n" +
        "        null,\n" +
        "        4,\n" +
        "        5,\n" +
        "        6,\n" +
        "        null\n" +
        "    ],\n" +
        "    \"responses\" : [\n" +
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

    String[] directives = new String[] {
      "set-column s0 json:select(body, true, \"$.name.fname\", \"$.name.lname\")",
      "set-column s1 json:select(body, true, \"$.name.fname\")",
      "set-column s11 json:select(body, true, \"$.numbers\")",
      "set-column s2 json:select(body, true, \"$.numbers\")",
      "set-column s4 json:drop(body, \"numbers\", \"integer\", \"float\", \"aliases\", \"name\")",
      "set-column s5 json:join(s11, \":\")"
    };

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
  }
}
