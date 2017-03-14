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

package co.cask.wrangler.steps.writer;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
import com.google.gson.Gson;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

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
    List<Record> records = Arrays.asList(
      new Record().add("int", 1).add("string", "this is string"),
      new Record("url", url)
      .add("o", o)
      .add("i1", 1)
      .add("i2", (double) 1.8f)
    );
    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 2);
    Assert.assertEquals(new Record("test", "{\"string\":\"this is string\",\"int\":1}"),
                        records.get(0));
    Assert.assertEquals(new Record("test", "{\"i1\":1,\"i2\":1.7999999523162842,\"url\":"
      + GSON.toJson(url) + ",\"o\":{\"map\":{\"a\":1,\"b\":\"2\"}}}"),
                        records.get(1));
  }
}
