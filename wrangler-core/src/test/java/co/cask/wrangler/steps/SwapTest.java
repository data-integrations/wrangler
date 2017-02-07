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
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.steps.column.Swap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Swap}
 */
public class SwapTest {

  @Test
  public void testSwap() throws Exception {
    String[] directives = new String[] {
      "swap a b",
    };

    List<Record> records = Arrays.asList(
      new Record("a", 1).add("b", "sample string")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals(1, records.get(0).getValue("b"));
    Assert.assertEquals("sample string", records.get(0).getValue("a"));
  }

  @Test(expected = StepException.class)
  public void testSwapFeildNotFound() throws Exception {
    String[] directives = new String[] {
      "swap a b",
    };

    List<Record> records = Arrays.asList(
      new Record("a", 1).add("c", "sample string")
    );

    records = PipelineTest.execute(directives, records);
  }

}
