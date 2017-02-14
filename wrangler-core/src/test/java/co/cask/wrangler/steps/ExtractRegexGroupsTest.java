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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link ExtractRegexGroups}
 */
public class ExtractRegexGroupsTest {

  @Test
  public void testRegexGroups() throws Exception {
    String[] directives = new String[] {
      "extract-regex-groups title [^(]+\\(([0-9]{4})\\).*",
    };

    List<Record> records = Arrays.asList(
      new Record("title", "Toy Story (1995)"),
      new Record("title", "Toy Story")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 2);
    Assert.assertEquals("1995", records.get(0).getValue(1));
    Assert.assertEquals(1, records.get(1).length());
  }
}