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
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.parser.TextDirectives;
import co.cask.wrangler.steps.transformation.MaskShuffle;
import co.cask.wrangler.steps.transformation.Split;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests different directives that are available within wrangling.
 */
public class PipelineTest {

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
  public void testMaskSuffle() throws Exception {
    List<Record> records = Arrays.asList(new Record("address", "150 Mars Street, Mar City, MAR, 783735"));
    Step step = new MaskShuffle(0, "", "address");
    Record actual = (Record) step.execute(records, null).get(0);
    Assert.assertEquals("089 Kyrp Czsyyr, Dyg Goci, FAG, 720322", actual.getValue("address"));
  }


  public static List<Record> execute(List<Step> steps, List<Record> records) throws StepException, ErrorRecordException {
    for (Step step : steps) {
      records = step.execute(records, null);
    }
    return records;
  }

  public static List<Record> execute(String[] directives, List<Record> records)
    throws StepException, DirectiveParseException, ErrorRecordException {
    TextDirectives specification = new TextDirectives(directives);
    List<Step> steps = new ArrayList<>();
    steps.addAll(specification.getSteps());
    records = PipelineTest.execute(steps, records);
    return records;
  }

  @Test
  public void testQuantizationRangeAndPattern() throws Exception {
    TreeMap<Double, String> rangeMap = new TreeMap<>();
    rangeMap.put(0.1, "A");
    rangeMap.put(0.9, null);
    rangeMap.put(2.0, "B");
    rangeMap.put(3.9, null);
    rangeMap.put(4.0, "C");
    rangeMap.put(5.9, null);

    String s = mappedValue(rangeMap, 2.2);
    Assert.assertEquals("B", s);

    Matcher m = Pattern.compile("([+-]?\\d+(?:\\.\\d+)?):([+-]?\\d+(?:\\.\\d+)?)=(.[^,]*)").matcher("0.9:2.1=Foo,2.2:3.4=9.2");
    TreeMap<String, String> stringRangeMap = new TreeMap<>();
    while(m.find()) {
      String lower = m.group(1);
      String upper = m.group(2);
      String value = m.group(3);
      stringRangeMap.put(lower, value);
      stringRangeMap.put(upper, null);
    }
    Assert.assertEquals("{0.9=Foo, 2.1=null, 2.2=9.2, 3.4=null}", stringRangeMap.toString());
  }

  private static <K, V> V mappedValue(TreeMap<K, V> rangeMap, K key) {
    Map.Entry<K, V> e = rangeMap.floorEntry(key);
    if (e != null && e.getValue() == null) {
      e = rangeMap.lowerEntry(key);
    }
    return e == null ? null : e.getValue();
  }

  @Test
  public void testSedGrep() throws Exception {
    String[] directives = new String[] {
      "find-and-replace body s/\"//g"
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
      "parse-as-csv body , false",
      "drop body",
      "rename body_1 date",
      "parse-as-csv date / false",
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

}

