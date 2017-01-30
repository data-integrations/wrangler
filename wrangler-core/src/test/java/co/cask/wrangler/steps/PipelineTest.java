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
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import nl.basjes.parse.core.Field;
import nl.basjes.parse.core.Parser;
import nl.basjes.parse.httpdlog.ApacheHttpdLoglineParser;
import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.unix4j.Unix4j;
import org.unix4j.unix.Cut;

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
    steps.add(new Split(0,"","col",",","firstCol","secondCol"));

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
    Step step = new Mask(0, "", "ssn", "xxx-xx-#####", 1);
    List<Record> actual = step.execute(record, null);
    Assert.assertEquals("xxx-xx-0000", actual.get(0).getValue("ssn"));

    step = new Mask(0, "", "ssn", "xxx-xx-####-0", 1);
    actual = step.execute(record, null);
    Assert.assertEquals("xxx-xx-0000-0", actual.get(0).getValue("ssn"));

    step = new Mask(0, "", "ssn", "xxx-xx-####", 1);
    actual = step.execute(record, null);
    Assert.assertEquals("xxx-xx-0000", actual.get(0).getValue("ssn"));
    step = new Mask(0, "", "ssn", "x-####", 1);
    actual = step.execute(record, null);
    Assert.assertEquals("x-8899", actual.get(0).getValue("ssn"));
  }

  @Test
  public void testMaskSuffle() throws Exception {
    List<Record> records = Arrays.asList(new Record("address", "150 Mars Street, Mar City, MAR, 783735"));
    Step step = new Mask(0, "", "address", "", 2);
    Record actual = (Record) step.execute(records, null).get(0);
    Assert.assertEquals("089 Kyrp Czsyyr, Dyg Goci, FAG, 720322", actual.getValue("address"));
  }

  @Test
  public void testDateFormat() throws Exception {
    List<Record> record = Arrays.asList(new Record("date", "01/06/2017"));

    Step step = new FormatDate(0, "", "date", "MM/dd/yyyy", "EEE, MMM d, ''yy");
    Record actual = (Record) step.execute(record, null).get(0);
    Assert.assertEquals("Fri, Jan 6, '17", actual.getValue("date"));

    step = new FormatDate(0, "", "date", "MM/dd/yyyy", "EEE, d MMM yyyy HH:mm:ss");
    actual = (Record) step.execute(record, null).get(0);
    Assert.assertEquals("Fri, 6 Jan 2017 00:00:00", actual.getValue("date"));

    step = new FormatDate(0, "", "date", "MM/dd/yyyy", "yyyy.MM.dd G 'at' HH:mm:ss");
    actual = (Record) step.execute(record, null).get(0);
    Assert.assertEquals("2017.01.06 AD at 00:00:00", actual.getValue("date"));

    record = Arrays.asList(new Record("unixtimestamp", "1483803222"));
    step = new FormatDate(0, "", "unixtimestamp", "EEE, MMM d, ''yy");
    actual = (Record) step.execute(record, null).get(0);
    Assert.assertEquals("Sat, Jan 7, '17", actual.getValue("unixtimestamp"));
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

    // Run through the wrangling steps.
    List<Record> records = Arrays.asList(new Record("__col", "1098,Root,Joltie,01/26/1956,root@jolite.io,32,11.79," +
      "150 Mars Ave,Palo Alto,CA,USA,32826"));

    // Iterate through steps.
    records = PipelineTest.execute(directives, records);

    Assert.assertEquals("Joltie, Root", records.get(0).getValue("name"));
    Assert.assertEquals("1886.3999999999999", records.get(0).getValue("salary"));
    Assert.assertEquals("no", records.get(0).getValue("isteen"));
    Assert.assertEquals("oi.etiloj@toor", records.get(0).getValue("email"));
    Assert.assertEquals("13.0", records.get(0).getValue("hrlywage"));
  }

  @Test(expected = StepException.class)
  public void testNegativeConditionApply() throws Exception {
    String[] directives = new String[] {
      "set format csv , false",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "set column email string:reverse(email1)"
    };

    TextDirectives specification = new TextDirectives(directives);

    List<Record> records = Arrays.asList(new Record("__col", "1098,Root,Joltie,01/26/1956,root@jolite.io," +
      "32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"));

    // Define all the steps in the wrangler.
    List<Step> steps = new ArrayList<>(specification.getSteps());

    // Run through the wrangling steps.
    records = PipelineTest.execute(directives, records);
  }

  @Test
  public void testRowFilterRegex() throws Exception {
    String[] directives = new String[] {
      "set format csv , false",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "filter-row-if-matched email .*@joltie.io",
      "filter-row-if-true id > 1092"
    };

    List<Record> records = Arrays.asList(
      new Record("__col", "1098,Root,Joltie,01/26/1956,root@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1091,Root,Joltie,01/26/1956,root1@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1092,Root,Joltie,01/26/1956,root@mars.com,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1093,Root,Joltie,01/26/1956,root@foo.com,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Record("__col", "1094,Super,Joltie,01/26/1956,windy@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826")
    );

    records = PipelineTest.execute(directives, records);

    // Filters all the records that don't match the pattern .*@joltie.io
    Assert.assertTrue(records.size() == 1);
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
  public void testParseJsonAndJsonPath() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body",
      "parse-as-json body.deviceReference",
      "parse-as-json body.deviceReference.OS",
      "parse-as-csv  body.deviceReference.screenSize | true",
      "drop body.deviceReference.screenSize",
      "rename body.deviceReference.screenSize_1 size1",
      "rename body.deviceReference.screenSize_2 size2",
      "rename body.deviceReference.screenSize_3 size3",
      "rename body.deviceReference.screenSize_4 size4",
      "json-path body.deviceReference.alerts signal_lost $.[*].['Signal lost']",
      "json-path signal_lost signal_lost $.[0]",
      "drop body",
      "drop body.deviceReference.OS",
      "drop body.deviceReference",
      "rename body.deviceReference.timestamp timestamp",
      "set column timestamp timestamp / 1000000",
      "drop body.deviceReference.alerts",
      "set columns timestamp,alerts,phone,battery,brand,type,comments,deviceId,os_name,os_version,size1,size2,size3,size4,signal"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "{ \"deviceReference\": { \"brand\": \"Samsung \", \"type\": \"Gear S3 frontier\", " +
        "\"deviceId\": \"SM-R760NDAAXAR\", \"timestamp\": 122121212341231, \"OS\": { \"name\": \"Tizen OS\", " +
        "\"version\": \"2.3.1\" }, \"alerts\": [ { \"Signal lost\": true }, { \"Emergency call\": true }, " +
        "{ \"Wifi connection lost\": true }, { \"Battery low\": true }, { \"Calories\": 354 } ], \"screenSize\": " +
        "\"extra-small|small|medium|large\", \"battery\": \"22%\", \"telephoneNumber\": \"+14099594986\", \"comments\": " +
        "\"It is an AT&T samung wearable device.\" } }")
      );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 1);
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

  @Test
  public void testCut() throws Exception {
    // cut column -c1,2,3
    // cut column -d " " -f1,2,3
    String s1 = Unix4j.fromString("one two three").cut("-c", "1-3").toStringResult();
    String s = Unix4j.fromString("some string another").cut(Cut.Options.fields, " ", 1,2).toStringResult();
    Assert.assertNotNull(s);
    Pattern p = Pattern.compile("^([0-9\\.,]*)(?:\\s*)?(.*)$");
    Matcher matcher = p.matcher("10Kb");
    String number = matcher.group(0);
    String unit = matcher.group(1);
    Assert.assertNotNull(p != null);
  }

  public final class MyRecord {
    private final Record record;

    public MyRecord(Record record) {
      this.record = record;
    }

    @Field({
      "VARIABLE:server.environment.unique_id",
      "HTTP.COOKIES:request.cookies.last",
      "HTTP.COOKIE:request.cookies.last.*",
      "STRING:request.status.original",
      "MICROSECONDS:response.server.processing.time",
      "HTTP.FIRSTLINE:request.firstline.original",
      "HTTP.METHOD:request.firstline.original.method",
      "HTTP.URI:request.firstline.original.uri",
      "HTTP.PROTOCOL:request.firstline.original.uri.protocol",
      "HTTP.USERINFO:request.firstline.original.uri.userinfo",
      "HTTP.HOST:request.firstline.original.uri.host",
      "HTTP.PORT:request.firstline.original.uri.port",
      "HTTP.PATH:request.firstline.original.uri.path",
      "HTTP.QUERYSTRING:request.firstline.original.uri.query",
      "STRING:request.firstline.original.uri.query.*",
      "HTTP.REF:request.firstline.original.uri.ref",
      "HTTP.PROTOCOL_VERSION:request.firstline.original.protocol",
      "HTTP.PROTOCOL:request.firstline.original.protocol",
      "HTTP.PROTOCOL.VERSION:request.firstline.original.protocol.version",
      "HTTP.COOKIES:request.cookies",
      "HTTP.COOKIE:request.cookies.*",
      "HTTP.USERAGENT:request.user-agent",
      "NUMBER:connection.client.logname",
      "MICROSECONDS:server.process.time",
      "HTTP.URI:request.referer.last",
      "HTTP.PROTOCOL:request.referer.last.protocol",
      "HTTP.USERINFO:request.referer.last.userinfo",
      "HTTP.HOST:request.referer.last.host",
      "HTTP.PORT:request.referer.last.port",
      "HTTP.PATH:request.referer.last.path",
      "HTTP.QUERYSTRING:request.referer.last.query",
      "STRING:request.referer.last.query.*",
      "HTTP.REF:request.referer.last.ref",
      "STRING:connection.client.user.last",
      "STRING:request.status",
      "HTTP.HEADER:request.header.host",
      "TIME.STAMP:request.receive.time",
      "TIME.DAY:request.receive.time.day",
      "TIME.MONTHNAME:request.receive.time.monthname",
      "TIME.MONTH:request.receive.time.month",
      "TIME.WEEK:request.receive.time.weekofweekyear",
      "TIME.YEAR:request.receive.time.weekyear",
      "TIME.YEAR:request.receive.time.year",
      "TIME.HOUR:request.receive.time.hour",
      "TIME.MINUTE:request.receive.time.minute",
      "TIME.SECOND:request.receive.time.second",
      "TIME.MILLISECOND:request.receive.time.millisecond",
      "TIME.DATE:request.receive.time.date",
      "TIME.TIME:request.receive.time.time",
      "TIME.ZONE:request.receive.time.timezone",
      "TIME.EPOCH:request.receive.time.epoch",
      "TIME.DAY:request.receive.time.day_utc",
      "TIME.MONTHNAME:request.receive.time.monthname_utc",
      "TIME.MONTH:request.receive.time.month_utc",
      "TIME.WEEK:request.receive.time.weekofweekyear_utc",
      "TIME.YEAR:request.receive.time.weekyear_utc",
      "TIME.YEAR:request.receive.time.year_utc",
      "TIME.HOUR:request.receive.time.hour_utc",
      "TIME.MINUTE:request.receive.time.minute_utc",
      "TIME.SECOND:request.receive.time.second_utc",
      "TIME.MILLISECOND:request.receive.time.millisecond_utc",
      "TIME.DATE:request.receive.time.date_utc",
      "TIME.TIME:request.receive.time.time_utc",
      "HTTP.HEADER:request.header.true-client-ip",
      "TIME.STAMP:request.receive.time.last",
      "TIME.DAY:request.receive.time.last.day",
      "TIME.MONTHNAME:request.receive.time.last.monthname",
      "TIME.MONTH:request.receive.time.last.month",
      "TIME.WEEK:request.receive.time.last.weekofweekyear",
      "TIME.YEAR:request.receive.time.last.weekyear",
      "TIME.YEAR:request.receive.time.last.year",
      "TIME.HOUR:request.receive.time.last.hour",
      "TIME.MINUTE:request.receive.time.last.minute",
      "TIME.SECOND:request.receive.time.last.second",
      "TIME.MILLISECOND:request.receive.time.last.millisecond",
      "TIME.DATE:request.receive.time.last.date",
      "TIME.TIME:request.receive.time.last.time",
      "TIME.ZONE:request.receive.time.last.timezone",
      "TIME.EPOCH:request.receive.time.last.epoch",
      "TIME.DAY:request.receive.time.last.day_utc",
      "TIME.MONTHNAME:request.receive.time.last.monthname_utc",
      "TIME.MONTH:request.receive.time.last.month_utc",
      "TIME.WEEK:request.receive.time.last.weekofweekyear_utc",
      "TIME.YEAR:request.receive.time.last.weekyear_utc",
      "TIME.YEAR:request.receive.time.last.year_utc",
      "TIME.HOUR:request.receive.time.last.hour_utc",
      "TIME.MINUTE:request.receive.time.last.minute_utc",
      "TIME.SECOND:request.receive.time.last.second_utc",
      "TIME.MILLISECOND:request.receive.time.last.millisecond_utc",
      "TIME.DATE:request.receive.time.last.date_utc",
      "TIME.TIME:request.receive.time.last.time_utc",
      "IP:connection.client.host",
      "STRING:connection.client.user",
      "HTTP.FIRSTLINE:request.firstline",
      "HTTP.METHOD:request.firstline.method",
      "HTTP.URI:request.firstline.uri",
      "HTTP.PROTOCOL:request.firstline.uri.protocol",
      "HTTP.USERINFO:request.firstline.uri.userinfo",
      "HTTP.HOST:request.firstline.uri.host",
      "HTTP.PORT:request.firstline.uri.port",
      "HTTP.PATH:request.firstline.uri.path",
      "HTTP.QUERYSTRING:request.firstline.uri.query",
      "STRING:request.firstline.uri.query.*",
      "HTTP.REF:request.firstline.uri.ref",
      "HTTP.PROTOCOL_VERSION:request.firstline.protocol",
      "HTTP.PROTOCOL:request.firstline.protocol",
      "HTTP.PROTOCOL.VERSION:request.firstline.protocol.version",
      "HTTP.URI:request.referer",
      "HTTP.PROTOCOL:request.referer.protocol",
      "HTTP.USERINFO:request.referer.userinfo",
      "HTTP.HOST:request.referer.host",
      "HTTP.PORT:request.referer.port",
      "HTTP.PATH:request.referer.path",
      "HTTP.QUERYSTRING:request.referer.query",
      "STRING:request.referer.query.*",
      "HTTP.REF:request.referer.ref",
      "MICROSECONDS:response.server.processing.time.original",
      "NUMBER:connection.client.logname.last",
      "HTTP.USERAGENT:request.user-agent.last",
      "IP:connection.client.host.last",
      "BYTES:response.body.bytesclf",
      "BYTESCLF:response.body.bytesclf",
      "BYTESCLF:response.body.bytes.last",
      "BYTES:response.body.bytes.last",
      "BYTESCLF:response.body.bytes",
      "BYTES:response.body.bytes"
    })

    public void setValue(final String name, final String value) {
      record.addOrSet(name, value);
    }
  }

  @Test
  public void testNattyDateParser() throws Exception {
//    Parser parser = new Parser();
//    List<DateGroup> groups = parser.parse("the day before next thursday");
//    for (DateGroup group : groups) {
//      List<Date> dates = group.getDates();
//      for (Date date : dates) {
//        System.out.println(date.toString());
//      }
//    }
//    Assert.assertNotNull(groups);
    String logformat = "%t %u [%D %h %{True-Client-IP}i %{UNIQUE_ID}e %r] %{Cookie}i %s \"%{User-Agent}i\" \"%{host}i\" %l %b %{Referer}i";
    String logline = "[02/Dec/2013:14:10:30 -0000] - [52075 10.102.4.254 177.43.52.210 UpyU1gpmBAwAACfd5W0AAAAW GET /SS14-VTam-ny_019.jpg.rendition.zoomable.jpg HTTP/1.1] hsfirstvisit=http%3A%2F%2Fwww.domain.com%2Fen-us||1372268254000; _opt_vi_3FNG8DZU=F870DCFD-CBA4-4B6E-BB58-4605A78EE71A; __ptca=145721067.0aDxsZlIuM48.1372279055.1379945057.1379950362.9; __ptv_62vY4e=0aDxsZlIuM48; __pti_62vY4e=0aDxsZlIuM48; __ptcz=145721067.1372279055.1.0.ptmcsr=(direct)|ptmcmd=(none)|ptmccn=(direct); __hstc=145721067.b86362bb7a1d257bfa2d1fb77e128a85.1372268254968.1379934256743.1379939561848.9; hubspotutk=b86362bb7a1d257bfa2d1fb77e128a85; USER_GROUP=julinho%3Afalse; has_js=1; WT_FPC=id=177.43.52.210-1491335248.30301337:lv=1385997780893:ss=1385997780893; dtCookie=1F2E0E1037589799D8D503EB8CFA12A1|_default|1; RM=julinho%3A5248423ad3fe062f06c54915e6cde5cb45147977; wcid=UpyKsQpmBAwAABURyNoAAAAS%3A35d8227ba1e8a9a9cebaaf8d019a74777c32b4c8; Carte::KerberosLexicon_getWGSN=82ae3dcd1b956288c3c86bdbed6ebcc0fd040e1e; UserData=Username%3AJULINHO%3AHomepage%3A1%3AReReg%3A0%3ATrialist%3A0%3ALanguage%3Aen%3ACcode%3Abr%3AForceReReg%3A0; UserID=1356673%3A12345%3A1234567890%3A123%3Accode%3Abr; USER_DATA=1356673%3Ajulinho%3AJulio+Jose%3Ada+Silva%3Ajulinho%40tecnoblu.com.br%3A0%3A1%3Aen%3Abr%3A%3AWGSN%3A1385990833.81925%3A82ae3dcd1b956288c3c86bdbed6ebcc0fd040e1e; MODE=FONTIS; SECTION=%2Fcontent%2Fsection%2Fhome.html; edge_auth=ip%3D177.43.52.210~expires%3D1385994522~access%3D%2Fapps%2F%2A%21%2Fbin%2F%2A%21%2Fcontent%2F%2A%21%2Fetc%2F%2A%21%2Fhome%2F%2A%21%2Flibs%2F%2A%21%2Freport%2F%2A%21%2Fsection%2F%2A%21%2Fwgsn%2F%2A~md5%3D90e73ee10161c1afacab12c6ea30b4ef; __utma=94539802.1793276213.1372268248.1385572390.1385990581.16; __utmb=94539802.52.9.1385991739764; __utmc=94539802; __utmz=94539802.1372268248.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); WT_FPC=id=177.43.52.210-1491335248.30301337:lv=1386000374581:ss=1386000374581; dtPC=-; NSC_wtfswfs_xfcgbsn40-41=ffffffff096e1a1d45525d5f4f58455e445a4a423660; akamai-edge=5ac6e5b3d0bbe2ea771bb2916d8bab34ea222a6a 200 \"Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36\" \"www.domain.com\" - 463952 http://www.domain.com/content/report/shows/New_York/KSHK/trip/s_s_14_ny_ww/sheers.html";

    Parser<MyRecord> parser = new ApacheHttpdLoglineParser<>(MyRecord.class, logformat);
    List<String> s = parser.getPossiblePaths();
    for (String s1 : s) {
      System.out.println(s1);
    }
    Record log = new Record();
    MyRecord record = new MyRecord(log);

    try {
      record = parser.parse(record, logline);
    } catch (Exception disectionFailure) {
      disectionFailure.printStackTrace();
    }

    Assert.assertNotNull(record);

  }

}

