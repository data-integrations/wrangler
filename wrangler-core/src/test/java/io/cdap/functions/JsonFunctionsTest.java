/*
 *  Copyright © 2020 Cask Data, Inc.
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

package io.cdap.functions;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.utils.JsonTestData;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for all the Json functions.
 */
public class JsonFunctionsTest {

  private static final String JSON_SELECTION_EG1 = "{\n" +
    "  \"list\" : [\n" +
    "    {\n" +
    "      \"set\" : [\n" +
    "        { \"a1\" : \"b1\" },\n" +
    "        { \"x1\" : \"y1\" }\n" +
    "      ],\n" +
    "      \"map\" : \"X1\",\n" +
    "      \"collection\" : \"Y1\"\n" +
    "    },\n" +
    "    {\n" +
    "      \"set\" : [\n" +
    "        { \"a2\" : \"b2\" },\n" +
    "        { \"x2\" : \"y2\" }\n" +
    "      ],\n" +
    "      \"map\" : \"X2\",\n" +
    "      \"collection\" : \"Y2\"      \n" +
    "    },\n" +
    "    {\n" +
    "      \"set\" : [\n" +
    "        { \"a3\" : \"b3\" },\n" +
    "        { \"x3\" : \"y3\" }\n" +
    "      ],\n" +
    "      \"map\" : \"X3\",\n" +
    "      \"collection\" : \"Y3\"      \n" +
    "    }\n" +
    "  ]\n" +
    "}";

  @Test
  public void testJsonSelect() throws Exception {
    String[] directives = new String[]{
      "set-column mayo json:Parse(body)",
      "drop boody",
      "set-column entries json:Select(mayo, '$.list[*].set.*')"
    };
    List<Row> rows = Arrays.asList(
      new Row("body", JSON_SELECTION_EG1)
    );
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
  }

  @Test
  public void testBasicJson() throws Exception {
    String[] directives = new String[]{
      "set-column baddata if(json:IsValid(malformed)) { json:Parse(malformed) } else { 'Invalid Json'}",
      "set-column badjson json:IsValid(malformed)",
      "set-column goodjson json:IsValid(basic)",
      "set-column basicparsed json:Parse(basic)"
    };

    List<Row> rows = Arrays.asList(
      new Row("malformed", JsonTestData.MALFORMED_BASIC_JSON).add("basic", JsonTestData.BASIC)
    );
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("Invalid Json", rows.get(0).getValue("baddata"));
    Assert.assertEquals(false, rows.get(0).getValue("badjson"));
    Assert.assertEquals(true, rows.get(0).getValue("goodjson"));
  }
}