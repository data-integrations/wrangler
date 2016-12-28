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
    Row row = new Row("col", "1,2,a,A,one name|p2|p3");

    // Define all the steps in the wrangler.
    steps.add(new CsvParser(0, "", new CsvParser.Options(), "col", true));
    steps.add(new Columns(0, "", Arrays.asList("first", "second", "third", "fourth", "fifth")));
    steps.add(new Rename(0, "", "first", "one"));
    steps.add(new Lower(0, "", "fourth"));
    steps.add(new Upper(0, "", "third"));
    steps.add(new CsvParser(0, "", new CsvParser.Options('|'), "fifth", false));
    steps.add(new Drop(0, "", "fifth"));
    steps.add(new Merge(0, "", "one", "second", "merged", "%"));
    steps.add(new Rename(0, "", "col5", "test"));
    steps.add(new TitleCase(0, "", "test"));
    steps.add(new IndexSplit(0, "", "test", 1, 4, "substr"));

    // Run through the wrangling steps.
    for (Step step : steps) {
      row = (Row) step.execute(row);
    }

    Assert.assertEquals("one", row.getColumn(0));
    Assert.assertEquals("A", row.getValue(2));
    Assert.assertEquals("a", row.getValue(3));
    Assert.assertEquals("One", row.getValue("substr"));

  }

  @Test
  public void testSplit() throws Exception {
    List<Step> steps = new ArrayList<>();
    Row row = new Row("col", "1,2,a,A");

    // Define all the steps in the wrangler.
    steps.add(new Split(0,"","col",",","firstCol","secondCol"));

    // Run through the wrangling steps.
    for (Step step : steps) {
      row = (Row) step.execute(row);
    }

    Assert.assertEquals("1", row.getValue("firstCol"));
    Assert.assertEquals("2,a,A", row.getValue("secondCol"));
  }

  @Test
  public void testSplitWithNull() throws Exception {
    List<Step> steps = new ArrayList<>();
    Row row = new Row("col", "1,2,a,A");

    // Define all the steps in the wrangler.
    steps.add(new Split(0,"","col","|","firstCol","secondCol"));

    // Run through the wrangling steps.
    for (Step step : steps) {
      row = (Row) step.execute(row);
    }

    Assert.assertEquals("1,2,a,A", row.getValue("firstCol"));
    Assert.assertNull(row.getValue("secondCol"));
  }

  @Test
  public void testMaskingSubstitution() throws Exception {
    // Check valid status.
    Row row = new Row("ssn", "888990000");
    Step step = new Mask(0, "", "ssn", "xxx-xx-####", 1);
    Row actual = (Row) step.execute(row);
    Assert.assertEquals("xxx-xx-0000", actual.getValue("ssn"));
    step = new Mask(0, "", "ssn", "x-####", 1);
    actual = (Row) step.execute(row);
    Assert.assertEquals("x-8899", actual.getValue("ssn"));
  }

  @Test
  public void testMaskSuffle() throws Exception {
    Row row = new Row("address", "150 Mars Street, Mar City, MAR, 783735");
    Step step = new Mask(0, "", "address", "", 2);
    Row actual = (Row) step.execute(row);
    Assert.assertEquals("089 Kyrp Czsyyr, Dyg Goci, FAG, 720322", actual.getValue("address"));
  }

}
