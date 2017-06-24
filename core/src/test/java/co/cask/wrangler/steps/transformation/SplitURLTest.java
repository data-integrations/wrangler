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

package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.RecipePipelineTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Test {@link SplitURL}
 */
public class SplitURLTest {

  @Test
  public void testBasicURLWorking() throws Exception {
    String[] directives = new String[] {
      "split-url url",
    };

    List<Record> records = Arrays.asList(
      new Record("url", "http://example.com:80/docs/books/tutorial/index.html?name=networking#DOWNLOADING")
    );

    records = RecipePipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals(80, records.get(0).getValue("url_port"));
    Assert.assertEquals("example.com", records.get(0).getValue("url_host"));
  }

}
