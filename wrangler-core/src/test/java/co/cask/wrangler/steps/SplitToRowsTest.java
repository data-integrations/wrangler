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
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SplitToRows}
 */
public class SplitToRowsTest {

  @Test
  public void testSplitToRows() throws Exception {
    String[] directives = new String[] {
      "split-to-rows body \\n",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "AABBCDE\nEEFFFF")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 2);
    Assert.assertEquals("AABBCDE", records.get(0).getValue("body"));
    Assert.assertEquals("EEFFFF", records.get(1).getValue("body"));
  }

  @Test
  public void testSplitWhenNoPatternMatch() throws Exception {
    String[] directives = new String[] {
      "split-to-rows body X",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "AABBCDE\nEEFFFF")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
  }

  @Test
  public void testDocExample() throws Exception {
    String[] directives = new String[] {
      "split-to-rows codes \\|",
    };

    List<Record> records = Arrays.asList(
      new Record("id", "1").add("codes", "USD|AUD|AMD|XCD")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 4);
  }

}