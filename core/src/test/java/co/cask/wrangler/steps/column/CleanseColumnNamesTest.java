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
import co.cask.wrangler.steps.RecipePipelineTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link CleanseColumnNames}
 */
public class CleanseColumnNamesTest {

  @Test
  public void testColumnCleanse() throws Exception {
    String[] directives = new String[] {
      "cleanse-column-names",
    };

    List<Record> records = Arrays.asList(
      new Record("COL1", "1").add("col:2", "2").add("Col3", "3").add("COLUMN4", "4").add("col!5", "5")
    );

    records = RecipePipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("col1", records.get(0).getColumn(0));
    Assert.assertEquals("col_2", records.get(0).getColumn(1));
    Assert.assertEquals("col3", records.get(0).getColumn(2));
    Assert.assertEquals("column4", records.get(0).getColumn(3));
    Assert.assertEquals("col_5", records.get(0).getColumn(4));
  }
}
