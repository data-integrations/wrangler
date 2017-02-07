/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.transformation.UrlEncode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link UrlEncode}
 */
public class UrlEncodeTest {

  @Test
  public void testUrlEncoding() throws Exception {
    String[] directives = new String[] {
      "url-encode url",
    };

    List<Record> records = Arrays.asList(
      new Record("url", "http://www.yahoo.com?a=b c&b=ab&xyz=1")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("http%3A%2F%2Fwww.yahoo.com%3Fa%3Db+c%26b%3Dab%26xyz%3D1", records.get(0).getValue("url"));
  }
}