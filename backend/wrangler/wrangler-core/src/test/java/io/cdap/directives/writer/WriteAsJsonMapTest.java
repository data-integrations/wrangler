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

package io.cdap.directives.writer;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link WriteAsJsonMap}
 */
public class WriteAsJsonMapTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testWriteToJson() throws Exception {
    String[] directives = new String[] {
      "write-as-json-map test",
      "keep test"
    };

    JSONObject o = new JSONObject();
    o.put("a", 1);
    o.put("b", "2");
    String url = "http://www.yahoo.com?a=b c&b=ab&xyz=1";
    List<Row> rows = Arrays.asList(
      new Row().add("int", 1).add("string", "this is string"),
      new Row("url", url)
      .add("o", o)
      .add("i1", 1)
      .add("i2", (double) 1.8f)
    );
    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 2);
    Type stringStringMapType = new TypeToken<Map<String, String>>() { }.getType();
    Map<String, String> map = GSON.fromJson((String) rows.get(0).getValue("test"), stringStringMapType);
    Assert.assertEquals(ImmutableMap.of("string", "this is string", "int", "1"), map);

    JsonObject jsonObject = new JsonParser().parse((String) rows.get(1).getValue("test")).getAsJsonObject();
    Assert.assertEquals(1, jsonObject.get("i1").getAsInt());

    Assert.assertEquals(1.8f, jsonObject.get("i2").getAsFloat(), 0.001);
    Assert.assertEquals(url, jsonObject.get("url").getAsString());
    Assert.assertEquals(GSON.toJson(o), jsonObject.get("o").toString());
  }
}
