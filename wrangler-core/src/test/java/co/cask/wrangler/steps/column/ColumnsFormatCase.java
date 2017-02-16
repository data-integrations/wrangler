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

package co.cask.wrangler.steps.column;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.steps.PipelineTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link ColumnsFormatCase}
 */
public class ColumnsFormatCase {

  @Test
  public void testColumnsFormatCase() throws Exception {
    String[] directives = new String[] {
      "columns-format-case lower_camel lower_underscore",
    };

    List<Record> records = Arrays.asList(
      new Record("someThing", 1).add("thisWay", 2).add("wickedComes", 3)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("some_thing", records.get(0).getColumn(0));
    Assert.assertEquals("this_way", records.get(0).getColumn(1));
    Assert.assertEquals("wicked_comes", records.get(0).getColumn(2));
  }
}
