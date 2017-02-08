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
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link WriteAsJsonMap}
 */
public class WriteAsJsonMapTest {

  @Test
  public void testWriteToJson() throws Exception {
    String[] directives = new String[] {
      "write-as-json-map test",
    };

    JSONObject o = new JSONObject();
    o.put("a", 1);
    o.put("b", "2");
    List<Record> records = Arrays.asList(
      new Record("url", "http://www.yahoo.com?a=b c&b=ab&xyz=1")
      .add("o", o)
      .add("i1", new Integer(1))
      .add("i2", new Double(1.8f))
    );
    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
  }
}
