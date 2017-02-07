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
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.steps.column.Keep;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Keep}
 */
public class KeepTest {

  @Test
  public void testKeep() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "keep body_1,body_2"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "1,2,3,4,5,6,7,8,9,10")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals(2, records.get(0).length());
  }

  @Test(expected = StepException.class)
  public void testKeepWithNoFieldInRecord() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "keep body_1,body_2",
      "keep body_3"  // this field body_3 is not existing in record anymore.
    };

    List<Record> records = Arrays.asList(
      new Record("body", "1,2,3,4,5,6,7,8,9,10")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals(2, records.get(0).length());
  }
}