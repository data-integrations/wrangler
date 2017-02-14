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
 * Tests {@link ColumnsReplace}
 */
public class ColumnsReplaceTest {

  @Test
  public void testBasicColumnReplace() throws Exception {
    String[] directives = new String[] {
      "columns-replace s/^data_//g",
    };

    List<Record> records = Arrays.asList(
      new Record("data_a", 1).add("data_b", 2).add("data_timestamp", 3).add("data_data_confuse", 4)
        .add("no_data", 5).add("whatever", 6)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("a", records.get(0).getColumn(0));
    Assert.assertEquals("b", records.get(0).getColumn(1));
    Assert.assertEquals("timestamp", records.get(0).getColumn(2));
    Assert.assertEquals("data_confuse", records.get(0).getColumn(3));
    Assert.assertEquals("no_data", records.get(0).getColumn(4));
    Assert.assertEquals("whatever", records.get(0).getColumn(5));
  }

  @Test(expected = StepException.class)
  public void testIncorrectSedExpression() throws Exception {
    String[] directives = new String[] {
      "columns-replace r/^data_//g", // Incorrect sed expression.
    };

    List<Record> records = Arrays.asList(
      new Record("data_a", 1).add("data_b", 2).add("data_timestamp", 3).add("data_data_confuse", 4)
        .add("no_data", 5).add("whatever", 6)
    );

    PipelineTest.execute(directives, records);
  }
}
