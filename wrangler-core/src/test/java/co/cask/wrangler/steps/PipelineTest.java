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

import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.internal.TextDirectives;
import co.cask.wrangler.steps.column.Columns;
import co.cask.wrangler.steps.column.Drop;
import co.cask.wrangler.steps.column.Merge;
import co.cask.wrangler.steps.column.Rename;
import co.cask.wrangler.steps.parser.CsvParser;
import co.cask.wrangler.steps.transformation.IndexSplit;
import co.cask.wrangler.steps.transformation.Lower;
import co.cask.wrangler.steps.transformation.Mask;
import co.cask.wrangler.steps.transformation.Split;
import co.cask.wrangler.steps.transformation.TitleCase;
import co.cask.wrangler.steps.transformation.Upper;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests different directives that are available within wrangling.
 */
public class PipelineTest {

  @Test
  public void testBasicPipelineWorking() throws Exception {
    List<Step> steps = new ArrayList<>();
    List<Record> records = Arrays.asList(new Record("col", "1,2,a,A,one name|p2|p3"));

    // Define all the steps in the wrangler.
    steps.add(new CsvParser(0, "", new CsvParser.Options(), "col", true));
    steps.add(new Columns(0, "", Arrays.asList("col", "first", "second", "third", "fourth", "fifth")));
    steps.add(new Rename(0, "", "first", "one"));
    steps.add(new Lower(0, "", "fourth"));
    steps.add(new Upper(0, "", "third"));
    steps.add(new CsvParser(0, "", new CsvParser.Options('|'), "fifth", false));
    steps.add(new Drop(0, "", "fifth"));
    steps.add(new Merge(0, "", "one", "second", "merged", "%"));
    steps.add(new Rename(0, "", "fifth_1", "test"));
    steps.add(new TitleCase(0, "", "test"));
    steps.add(new IndexSplit(0, "", "test", 1, 4, "substr"));

    // Run through the wrangling steps.
    for (Step step : steps) {
      records = step.execute(new ArrayList<Record>(records), null);
    }

    Assert.assertEquals("one", records.get(0).getColumn(1));
    Assert.assertEquals("A", records.get(0).getValue(3));
    Assert.assertEquals("a", records.get(0).getValue(4));
    Assert.assertEquals("One", records.get(0).getValue("substr"));

  }

  @Test
  public void testSplit() throws Exception {
    List<Step> steps = new ArrayList<>();
    List<Record> records = Arrays.asList(new Record("col", "1,2,a,A"));

    // Define all the steps in the wrangler.
    steps.add(new Split(0, "", "col", ",", "firstCol", "secondCol"));

    // Run through the wrangling steps.
    for (Step step : steps) {
      records = step.execute(records, null);
    }

    Assert.assertEquals("1", records.get(0).getValue("firstCol"));
    Assert.assertEquals("2,a,A", records.get(0).getValue("secondCol"));
  }

  @Ignore
  @Test
  public void testSplitWithNull() throws Exception {
    List<Record> records = Arrays.asList(new Record("col", "1,2,a,A"));

    // Define all the steps in the wrangler.
    Step step = new Split(0,"","col","|","firstCol","secondCol");

    // Run through the wrangling steps.
    List<Record> actual = step.execute(records, null);

    Assert.assertEquals("1,2,a,A", actual.get(0).getValue("firstCol"));
    Assert.assertNull(actual.get(0).getValue("secondCol"));
  }

  @Test
  public void testMaskingSubstitution() throws Exception {
    // Check valid status.
    List<Record> record = Arrays.asList(new Record("ssn", "888990000"));

    // More characters in mask, but not enough in the input.
    Step step = new MaskNumber(0, "", "ssn", "xxx-xx-#####");
    List<Record> actual = step.execute(record, null);
    Assert.assertEquals("xxx-xx-0000", actual.get(0).getValue("ssn"));

    step = new MaskNumber(0, "", "ssn", "xxx-xx-####-0");
    actual = step.execute(record, null);
    Assert.assertEquals("xxx-xx-0000-0", actual.get(0).getValue("ssn"));

    step = new MaskNumber(0, "", "ssn", "xxx-xx-####");
    actual = step.execute(record, null);
    Assert.assertEquals("xxx-xx-0000", actual.get(0).getValue("ssn"));
    step = new MaskNumber(0, "", "ssn", "x-####");
    actual = step.execute(record, null);
    Assert.assertEquals("x-8899", actual.get(0).getValue("ssn"));
  }

  @Test
  public void testMaskSuffle() throws Exception {
    List<Record> records = Arrays.asList(new Record("address", "150 Mars Street, Mar City, MAR, 783735"));
    Step step = new MaskShuffle(0, "", "address");
    Record actual = (Record) step.execute(records, null).get(0);
    Assert.assertEquals("089 Kyrp Czsyyr, Dyg Goci, FAG, 720322", actual.getValue("address"));
  }


  public static List<Record> execute(List<Step> steps, List<Record> records) throws StepException {
    for (Step step : steps) {
      records = step.execute(records, null);
    }
    return records;
  }

  public static List<Record> execute(String[] directives, List<Record> records)
    throws StepException, DirectiveParseException {
    TextDirectives specification = new TextDirectives(directives);
    List<Step> steps = new ArrayList<>();
    steps.addAll(specification.getSteps());
    records = PipelineTest.execute(steps, records);
    return records;
  }

  @Test
  public void testEscapedStrings() throws Exception {
    List<Step> steps = new ArrayList<>();
    List<Record> records = Arrays.asList(new Record("__col", StringEscapeUtils.unescapeJava("1\\ta")));

    TextDirectives ts = new TextDirectives("set format csv \\t false\n" +
                                                   "set columns column1,column2\n" +
                                                   "rename column1 id\n" +
                                                   "rename column2 useragent\n" +
                                                   "uppercase useragent");
    // Define all the steps in the wrangler.
    steps.addAll(ts.getSteps());

    // Run through the wrangling steps.
    records = execute(steps, records);

    Assert.assertEquals("1", records.get(0).getValue("id"));
    Assert.assertEquals("A", records.get(0).getValue("useragent"));
  }

  @Test
  public void testQuantizationRangeAndPattern() throws Exception {
    RangeMap<Double, String> rangeMap = TreeRangeMap.create();
    rangeMap.put(Range.closed(0.1, 0.9), "A");
    rangeMap.put(Range.closed(2.0, 3.9), "B");
    rangeMap.put(Range.closed(4.0, 5.9), "C");
    String s = rangeMap.get(2.2);
    Assert.assertEquals("B", s);

    Matcher m = Pattern.compile("([+-]?\\d+(?:\\.\\d+)?):([+-]?\\d+(?:\\.\\d+)?)=(.[^,]*)").matcher("0.9:2.1=Foo,2.2:3.4=9.2");
    RangeMap<String, String> rm = TreeRangeMap.create();
    while(m.find()) {
      String lower = m.group(1);
      String upper = m.group(2);
      String value = m.group(3);
      rm.put(Range.closed(lower, upper), value);
    }
    Assert.assertEquals("[[0.9‥2.1]=Foo, [2.2‥3.4]=9.2]", rm.toString());
  }

  @Test
  public void testQuanitization() throws Exception {
    String[] directives = new String[] {
      "set format csv , false",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "quantize hrlywage wagerange 0.0:20.0=LOW,21.0:75.0=MEDIUM,75.1:200.0=HIGH",
      "set column wagerange (wagerange == null) ? \"NOT FOUND\" : wagerange"
    };

    List<Record> records = Arrays.asList(
      new Record("__col", "1098,Root,Joltie,01/26/1956,root@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1091,Root,Joltie,01/26/1956,root1@joltie.io,32,129.13,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1092,Root,Joltie,01/26/1956,root@mars.com,32,9.54,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1093,Root,Joltie,01/26/1956,root@foo.com,32,7.89,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1094,Root,Joltie,01/26/1956,windy@joltie.io,32,45.67,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1094,Root,Joltie,01/26/1956,windy@joltie.io,32,20.7,150 Mars Ave,Palo Alto,CA,USA,32826")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 6);
    int low = 0, medium = 0, high = 0, notfound = 0;
    for (Record record : records) {
      String v = (String) record.getValue("wagerange");
      if (v.equalsIgnoreCase("NOT FOUND")) {
        notfound++;
      } else if (v.equals("LOW")) {
        low++;
      } else if (v.equals("MEDIUM")) {
        medium++;
      } else if (v.equals("HIGH")) {
        high++;
      }
    }

    Assert.assertEquals(3, low);
    Assert.assertEquals(1, medium);
    Assert.assertEquals(1, high);
    Assert.assertEquals(1, notfound);
  }

  @Test
  public void testSedGrep() throws Exception {
    String[] directives = new String[] {
      "sed body s/\"//g"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "07/29/2013,Debt collection,\"Other (i.e. phone, health club, etc.)\",Cont'd attempts collect " +
        "debt not owed,Debt is not mine,,,\"NRA Group, LLC\",VA,20147,,N/A,Web,08/07/2013,Closed with non-monetary " +
        "relief,Yes,No,467801"),
      new Record("body", "07/29/2013,Mortgage,Conventional fixed mortgage,\"Loan servicing, payments, escrow account\",," +
        ",,Franklin Credit Management,CT,06106,,N/A,Web,07/30/2013,Closed with explanation,Yes,No,475823")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 2);
    Assert.assertEquals("07/29/2013,Debt collection,Other (i.e. phone, health club, etc.),Cont'd " +
                          "attempts collect debt not owed,Debt is not mine,,,NRA Group, LLC,VA,20147,,N/A," +
                          "Web,08/07/2013,Closed with non-monetary relief,Yes,No,467801",
                        records.get(0).getValue("body"));
  }

  @Test
  public void testParseCSV() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "drop body",
      "rename body_1 date",
      "parse-as-csv date / true",
      "rename date_1 month",
      "rename date_2 day",
      "rename date_3 year"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "07/29/2013,Debt collection,\"Other (i.e. phone, health club, etc.)\",Cont'd attempts collect " +
        "debt not owed,Debt is not mine,,,\"NRA Group, LLC\",VA,20147,,N/A,Web,08/07/2013,Closed with non-monetary " +
        "relief,Yes,No,467801"),
      new Record("body", "07/29/2013,Mortgage,Conventional fixed mortgage,\"Loan servicing, payments, escrow account\",," +
        ",,Franklin Credit Management,CT,06106,,N/A,Web,07/30/2013,Closed with explanation,Yes,No,475823")
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 2);
    Assert.assertEquals("07/29/2013", records.get(0).getValue("date"));
  }

  @Test
  public void testSplitToColumns() throws Exception {
    String[] directives = new String[] {
      "split-to-columns body \\n",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "AABBCDE\nEEFFFF")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("AABBCDE", records.get(0).getValue("body_1"));
    Assert.assertEquals("EEFFFF", records.get(0).getValue("body_2"));
  }

  @Test
  public void testFixedLengthParser() throws Exception {
    String[] directives = new String[] {
      "parse-as-fixed-length body 2,2,1,1,3,4",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "AABBCDEEEFFFF")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("AA", records.get(0).getValue("body_1"));
    Assert.assertEquals("BB", records.get(0).getValue("body_2"));
    Assert.assertEquals("C", records.get(0).getValue("body_3"));
    Assert.assertEquals("D", records.get(0).getValue("body_4"));
    Assert.assertEquals("EEE", records.get(0).getValue("body_5"));
    Assert.assertEquals("FFFF", records.get(0).getValue("body_6"));
  }

  @Test(expected = DirectiveParseException.class)
  public void testFixedLengthParserBadRangeSpecification() throws Exception {
    String[] directives = new String[] {
      "parse-as-fixed-length body A-B,C-D,12",
    };

    TextDirectives specification = new TextDirectives(directives);
    List<Step> steps = new ArrayList<>();
    steps.addAll(specification.getSteps());
  }

  @Test
  public void testFixedLengthWidthPadding() throws Exception {
    String[] directives = new String[] {
      "parse-as-fixed-length body 4,4,4,4,4,4 _" ,
    };

    List<Record> records = Arrays.asList(
      new Record("body", "AA__BB__C___D___EEE_FFFF")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("AA", records.get(0).getValue("body_1"));
    Assert.assertEquals("BB", records.get(0).getValue("body_2"));
    Assert.assertEquals("C", records.get(0).getValue("body_3"));
    Assert.assertEquals("D", records.get(0).getValue("body_4"));
    Assert.assertEquals("EEE", records.get(0).getValue("body_5"));
    Assert.assertEquals("FFFF", records.get(0).getValue("body_6"));
  }
}

