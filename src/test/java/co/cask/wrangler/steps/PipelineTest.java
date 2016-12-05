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

import co.cask.wrangler.api.ColumnType;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Step;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Wrangler Pipeline Testing.
 */
public class PipelineTest {

  @Test
  public void testBasicPipelineWorking() throws Exception {
    List<Step> steps = new ArrayList<>();
    Row row = new Row("col", ColumnType.STRING, "1,2,a,A,one name|p2|p3");

    // Define all the steps in the wrangler.
    steps.add(new CsvParser(new CsvParser.Options(), "col", true));
    steps.add(new Columns(Arrays.asList("first", "second", "third", "fourth", "fifth")));
    steps.add(new Types(Arrays.asList(ColumnType.STRING, ColumnType.STRING,
                                      ColumnType.STRING, ColumnType.STRING,
                                      ColumnType.STRING)));
    steps.add(new Rename("first", "one"));
    steps.add(new Lower("fourth"));
    steps.add(new Upper("third"));
    steps.add(new CsvParser(new CsvParser.Options('|'), "fifth", false));
    steps.add(new Drop("fifth"));
    steps.add(new Merge("one", "second", "merged", "%"));
    steps.add(new Rename("col5", "test"));
    steps.add(new TitleCase("test"));
    steps.add(new IndexSplit("test", 1, 4, "substr"));

    // Run through the wrangling steps.
    for (Step step : steps) {
      row = step.execute(row);
    }

    Assert.assertEquals("one", row.getName(0));
    Assert.assertEquals("A", row.getString(2));
    Assert.assertEquals("a", row.getString(3));
    Assert.assertEquals("One", row.get("substr"));

  }
}
