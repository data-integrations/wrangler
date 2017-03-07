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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.common.Bytes;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.steps.PipelineTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Expression}
 */
public class ExpressionTest {

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
      "set column hrlywage var x; x = math:ceil(FLOAT(hrlywage)); x + 1",
    };

    // Run through the wrangling steps.
    List<Record> records = Arrays.asList(new Record("__col", "1098,Root,Joltie,01/26/1956,root@jolite.io,32,11.79," +
      "150 Mars Ave,Palo Alto,CA,USA,32826"));

    // Iterate through steps.
    records = PipelineTest.execute(directives, records);

    Assert.assertEquals("Joltie, Root", records.get(0).getValue("name"));
    Assert.assertEquals(1886.3999999999999, records.get(0).getValue("salary"));
    Assert.assertEquals("no", records.get(0).getValue("isteen"));
    Assert.assertEquals("oi.etiloj@toor", records.get(0).getValue("email"));
    Assert.assertEquals(13.0, records.get(0).getValue("hrlywage"));
  }

  @Test(expected = StepException.class)
  public void testNegativeConditionApply() throws Exception {
    String[] directives = new String[] {
      "set format csv , false",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "set column email string:reverse(email1)" // email1 not defined in the record.
    };

    List<Record> records = Arrays.asList(new Record("__col", "1098,Root,Joltie,01/26/1956,root@jolite.io," +
      "32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"));

    // Run through the wrangling steps.
    PipelineTest.execute(directives, records);
  }

  @Test
  public void testStringNamespace() throws Exception {
    String[] directives = new String[] {
      "set column abbreviate string:abbreviate(longtxt, 5)",
      "set column center string:center(first, 10)",
      "set column eoltxt string:chomp(eoltxt)",
      "set column chop   string:chop(chop)",
      "set column delws  string:deleteWhitespace(delws)"
    };

    List<Record> records = Arrays.asList(
      new Record("number", "1")
            .add("first", "root")
            .add("last", "joltie")
            .add("longtxt", "This is long transformation")
            .add("eoltxt", "This has eol\n")
            .add("chop", "Joltie")
            .add("delws", "Jolti  Root")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("Jolti", records.get(0).getValue("chop"));
    Assert.assertEquals("JoltiRoot", records.get(0).getValue("delws"));
    Assert.assertEquals("This has eol", records.get(0).getValue("eoltxt"));
    Assert.assertEquals("Th...", records.get(0).getValue("abbreviate"));
  }

  @Test
  public void testBytesNamespace() throws Exception {
    String[] directives = new String[] {
      "set column first bytes:toString(first)",
      "set column number bytes:toInt(number)"
    };

    List<Record> records = Arrays.asList(
      new Record("number", Bytes.toBytes(99))
        .add("first", "root".getBytes(StandardCharsets.UTF_8))
        .add("last", "joltie".getBytes(StandardCharsets.UTF_8))
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("root", records.get(0).getValue("first"));
    Assert.assertEquals(99, records.get(0).getValue("number"));
  }

  @Test
  public void testDateFunctions() throws Exception {
    String[] directives = new String[] {
      "parse-as-simple-date date yyyy-MM-dd'T'HH:mm:ss",
      "parse-as-simple-date other yyyy-MM-dd'T'HH:mm:ss",
      "set-column unixtimestamp date:UNIXTIMESTAMP_MILLIS(date)",
      "set-column month_no date:MONTH(date)",
      "set-column month_short date:MONTH_SHORT(date)",
      "set-column month_long date:MONTH_LONG(date)",
      "set-column year date:YEAR(date)",
      "set-column day_of_year date:DAY_OF_YEAR(date)",
      "set-column era_long date:ERA_LONG(date)",
      "set-column days date:SECONDS_TO_DAYS(seconds)",
      "set-column hours date:SECONDS_TO_HOURS(seconds)",
      "set-column diff_days date:DAYS_BETWEEN_NOW(date)",
      "set-column diff date:DAYS_BETWEEN(date, other)"
    };


    //2017-02-02T21:06:44Z
    List<Record> records = Arrays.asList(
      new Record("date", "2017-02-02T21:06:44Z").add("seconds", 86401).add("other", "2017-02-03T21:06:44Z")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
  }

  @Test
  public void testJSONFunctions() throws Exception {
    String[] directives = new String[] {
      "parse-as-json body",
      "set-column max json:ARRAY_MAX(body_numbers)",
      "set-column min json:ARRAY_MIN(body_numbers)",
      "set-column sum json:ARRAY_SUM(body_numbers)",
      "set-column length json:ARRAY_LENGTH(body_numbers)",
      "set-column body_responses json:ARRAY_OBJECT_REMOVE_NULL_FIELDS(body_responses, \"c\")",
      "set-column body_responses json:ARRAY_OBJECT_RENAME_FIELDS(body_responses, \"a:field,b:value\")",
      "set-column body_responses json:ARRAY_OBJECT_DROP_FIELDS(body_responses, \"c\")"
    };

    //2017-02-02T21:06:44Z
    List<Record> records = Arrays.asList(
      new Record("body", "{\n" +
        "    \"name\" : {\n" +
        "        \"fname\" : \"Joltie\",\n" +
        "        \"lname\" : \"Root\",\n" +
        "        \"mname\" : null\n" +
        "    },\n" +
        "    \"coordinates\" : [\n" +
        "        12.56,\n" +
        "        45.789\n" +
        "    ],\n" +
        "    \"numbers\" : [\n" +
        "        1,\n" +
        "        2.1,\n" +
        "        3,\n" +
        "        null,\n" +
        "        4,\n" +
        "        5,\n" +
        "        6,\n" +
        "        null\n" +
        "    ],\n" +
        "    \"responses\" : [\n" +
        "        { \"a\" : 1, \"b\" : \"X\", \"c\" : 2.8},\n" +
        "        { \"a\" : 2, \"b\" : \"Y\", \"c\" : 232342.8},\n" +
        "        { \"a\" : 3, \"b\" : \"Z\", \"c\" : null},\n" +
        "        { \"a\" : 4, \"b\" : \"U\"}\n" +
        "    ],\n" +
        "    \"integer\" : 1,\n" +
        "    \"double\" : 2.8,\n" +
        "    \"float\" : 45.6,\n" +
        "    \"aliases\" : [\n" +
        "        \"root\",\n" +
        "        \"joltie\",\n" +
        "        \"bunny\",\n" +
        "        null\n" +
        "    ]\n" +
        "}")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals(21.1, records.get(0).getValue("sum"));
    Assert.assertEquals(6.0, records.get(0).getValue("max"));
    Assert.assertEquals(1.0, records.get(0).getValue("min"));
  }


}
