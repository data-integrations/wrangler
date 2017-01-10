/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import co.cask.wrangler.api.SkipRowException;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.internal.TextSpecification;
import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests different directives that are available within wrangling.
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

  @Ignore
  @Test
  public void testSplitWithNull() throws Exception {
    Row row = new Row("col", "1,2,a,A");

    // Define all the steps in the wrangler.
    Step step = new Split(0,"","col","|","firstCol","secondCol");

    // Run through the wrangling steps.
    Row actual = (Row) step.execute(row);

    Assert.assertEquals("1,2,a,A", actual.getValue("firstCol"));
    Assert.assertNull(actual.getValue("secondCol"));
  }

  @Test
  public void testMaskingSubstitution() throws Exception {
    // Check valid status.
    Row row = new Row("ssn", "888990000");

    // More characters in mask, but not enough in the input.
    Step step = new Mask(0, "", "ssn", "xxx-xx-#####", 1);
    Row actual = (Row) step.execute(row);
    Assert.assertEquals("xxx-xx-0000", actual.getValue("ssn"));

    step = new Mask(0, "", "ssn", "xxx-xx-####-0", 1);
    actual = (Row) step.execute(row);
    Assert.assertEquals("xxx-xx-0000-0", actual.getValue("ssn"));

    step = new Mask(0, "", "ssn", "xxx-xx-####", 1);
    actual = (Row) step.execute(row);
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

  @Test
  public void testDateFormat() throws Exception {
    Row row = new Row("date", "01/06/2017");

    Step step = new FormatDate(0, "", "date", "MM/dd/yyyy", "EEE, MMM d, ''yy");
    Row actual = (Row) step.execute(row);
    Assert.assertEquals("Fri, Jan 6, '17", actual.getValue("date"));

    step = new FormatDate(0, "", "date", "MM/dd/yyyy", "EEE, d MMM yyyy HH:mm:ss Z");
    actual = (Row) step.execute(row);
    Assert.assertEquals("Fri, 6 Jan 2017 00:00:00 -0800", actual.getValue("date"));

    step = new FormatDate(0, "", "date", "MM/dd/yyyy", "yyyy.MM.dd G 'at' HH:mm:ss z");
    actual = (Row) step.execute(row);
    Assert.assertEquals("2017.01.06 AD at 00:00:00 PST", actual.getValue("date"));

    row = new Row("unixtimestamp", "1483803222");
    step = new FormatDate(0, "", "unixtimestamp", "EEE, MMM d, ''yy");
    actual = (Row) step.execute(row);
    Assert.assertEquals("Sat, Jan 7, '17", actual.getValue("unixtimestamp"));
  }

  @Test
  public void testEscapedStrings() throws Exception {
    List<Step> steps = new ArrayList<>();
    Row row = new Row("__col", StringEscapeUtils.unescapeJava("1\\ta"));

    TextSpecification ts = new TextSpecification("set format csv \\t false\n" +
                                                   "set columns column1,column2\n" +
                                                   "rename column1 id\n" +
                                                   "rename column2 useragent\n" +
                                                   "uppercase useragent");
    // Define all the steps in the wrangler.
    steps.addAll(ts.getSteps());

    // Run through the wrangling steps.
    for (Step step : steps) {
      row = (Row) step.execute(row);
    }

    Assert.assertEquals("1", row.getValue("id"));
    Assert.assertEquals("A", row.getValue("useragent"));
  }

  @Test
  public void testApplyExpr() throws Exception {
    String[] directives = new String[] {
      "set format csv , false",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "set column name concat(last, \", \", first)",
      "set column isteen age < 15 ? 'yes' : 'no'",
      "set column salary hrlywage*40*4",
      "drop first",
      "drop last",
      "set column email string:reverse(email)",
      "set column hrlywage var x; x = math:ceil(toFloat(hrlywage)); x + 1",
      "format-date dob MM/dd/YYYY EEE, d MMM yyyy HH:mm:ss Z"
    };
    TextSpecification specification = new TextSpecification(directives);

    // Define all the steps in the wrangler.
    List<Step> steps = new ArrayList<>(specification.getSteps());

    // Run through the wrangling steps.
    Row row = new Row("__col", "1098,Root,Joltie,01/26/1956,root@jolite.io,32,11.79," +
      "150 Mars Ave,Palo Alto,CA,USA,32826");

    // Iterate through steps.
    for (Step step : steps) {
      row = (Row) step.execute(row);
    }

    Assert.assertEquals("Joltie, Root", row.getValue("name"));
    Assert.assertEquals("1886.3999999999999", row.getValue("salary"));
    Assert.assertEquals("no", row.getValue("isteen"));
    Assert.assertEquals("oi.etiloj@toor", row.getValue("email"));
    Assert.assertEquals("13.0", row.getValue("hrlywage"));
  }

  @Test(expected = StepException.class)
  public void testNegativeConditionApply() throws Exception {
    String[] directives = new String[] {
      "set format csv , false",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "set column email string:reverse(email1)"
    };

    TextSpecification specification = new TextSpecification(directives);

    Row row = new Row("__col", "1098,Root,Joltie,01/26/1956,root@jolite.io,32,11.79,150 Mars Ave," +
      "Palo Alto,CA,USA,32826");

    // Define all the steps in the wrangler.
    List<Step> steps = new ArrayList<>(specification.getSteps());

    // Run through the wrangling steps.
    for (Step step : steps) {
      row = (Row) step.execute(row);
    }
  }

  @Test
  public void testRowFilterRegex() throws Exception {
    String[] directives = new String[] {
      "set format csv , false",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "filter-row-by-regex email .*@joltie.io",
      "filter-row-by-condition id > 1092"
    };

    Row[] rows = new Row[] {
      new Row("__col", "1098,Root,Joltie,01/26/1956,root@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1091,Root,Joltie,01/26/1956,root1@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1092,Root,Joltie,01/26/1956,root@mars.com,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1093,Root,Joltie,01/26/1956,root@foo.com,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1094,Root,Joltie,01/26/1956,windy@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826")
    };

    TextSpecification specification = new TextSpecification(directives);
    List<Step> steps = new ArrayList<>();
    steps.addAll(specification.getSteps());

    List<Row> actuals = new ArrayList<>();
    for (Row row : rows) {
      Row r = row;
      try {
        for (Step step : steps) {
          r = (Row) step.execute(r);
        }
      } catch (SkipRowException e) {
        continue;
      }
      actuals.add(r);
    }

    // Filters all the rows that don't match the pattern .*@joltie.io
    Assert.assertTrue(actuals.size() == 1);
  }

}
