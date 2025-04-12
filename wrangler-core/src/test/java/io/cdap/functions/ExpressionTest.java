/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.functions;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.directives.transformation.ColumnExpression;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link ColumnExpression}
 */
public class ExpressionTest {

  @Test
  public void testRowNoOfColumns() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ','",
      "set-column :cols exp:{ this.width() }"
    };

    List<Row> rows = Arrays.asList(new Row("body", "1098,Root,Joltie,01/26/1956,root@jolite.io,32,11.79," +
      "150 Mars Ave,Palo Alto,CA,USA,32826"));

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(13, rows.get(0).getValue("cols"));
  }

  @Test
  public void testApplyExpr() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv __col ,",
      "drop __col",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "set column name concat(last, \", \", first)",
      "set column isteen age < 15 ? 'yes' : 'no'",
      "set column salary hrlywage*40*4",
      "drop first",
      "drop last",
      "set column email string:reverse(email)",
      "set column hrlywage var x; x = math:ceil(toFloat(hrlywage)); x + 1",
    };

    // Run through the wrangling steps.
    List<Row> rows = Arrays.asList(new Row("__col", "1098,Root,Joltie,01/26/1956,root@jolite.io,32,11.79," +
      "150 Mars Ave,Palo Alto,CA,USA,32826"));

    // Iterate through steps.
    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals("Joltie, Root", rows.get(0).getValue("name"));
    Assert.assertEquals(1886.3999999999999, rows.get(0).getValue("salary"));
    Assert.assertEquals("no", rows.get(0).getValue("isteen"));
    Assert.assertEquals("oi.etiloj@toor", rows.get(0).getValue("email"));
    Assert.assertEquals(13.0, rows.get(0).getValue("hrlywage"));
  }

  @Test
  public void testNegativeConditionApply() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv __col ,",
      "drop __col",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "set column email string:reverse(email1)" // email1 not defined in the record.
    };

    List<Row> rows = Arrays.asList(new Row("__col", "1098,Root,Joltie,01/26/1956,root@jolite.io," +
      "32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"));

    // Run through the wrangling steps.
    TestingRig.execute(directives, rows);
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

    List<Row> rows = Arrays.asList(
      new Row("number", "1")
            .add("first", "root")
            .add("last", "joltie")
            .add("longtxt", "This is long transformation")
            .add("eoltxt", "This has eol\n")
            .add("chop", "Joltie")
            .add("delws", "Jolti  Root")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("Jolti", rows.get(0).getValue("chop"));
    Assert.assertEquals("JoltiRoot", rows.get(0).getValue("delws"));
    Assert.assertEquals("This has eol", rows.get(0).getValue("eoltxt"));
    Assert.assertEquals("Th...", rows.get(0).getValue("abbreviate"));
  }

  @Test
  public void testBytesNamespace() throws Exception {
    String[] directives = new String[] {
      "set column first bytes:toString(first)",
      "set column number bytes:toInt(number)"
    };

    List<Row> rows = Arrays.asList(
      new Row("number", Bytes.toBytes(99))
        .add("first", "root".getBytes(StandardCharsets.UTF_8))
        .add("last", "joltie".getBytes(StandardCharsets.UTF_8))
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("root", rows.get(0).getValue("first"));
    Assert.assertEquals(99, rows.get(0).getValue("number"));
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
    List<Row> rows = Arrays.asList(
      new Row("date", "2017-02-02T21:06:44Z").add("seconds", 86401).add("other", "2017-02-03T21:06:44Z")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);

    ZonedDateTime date = ZonedDateTime.of(2017, 2, 2, 21, 6, 44, 0, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    ZonedDateTime other = ZonedDateTime.of(2017, 2, 3, 21, 6, 44, 0, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    Assert.assertEquals(date, rows.get(0).getValue("date"));
    Assert.assertEquals(other, rows.get(0).getValue("other"));
    Assert.assertEquals(1486069604000L, rows.get(0).getValue("unixtimestamp"));
    Assert.assertEquals(2, rows.get(0).getValue("month_no"));
    Assert.assertEquals("Feb", rows.get(0).getValue("month_short"));
    Assert.assertEquals("February", rows.get(0).getValue("month_long"));
    Assert.assertEquals(2017, rows.get(0).getValue("year"));
    Assert.assertEquals(33, rows.get(0).getValue("day_of_year"));
    Assert.assertEquals("AD", rows.get(0).getValue("era_long"));
    Assert.assertEquals(1, rows.get(0).getValue("days"));
    Assert.assertEquals(24, rows.get(0).getValue("hours"));
    Assert.assertEquals(1, rows.get(0).getValue("diff"));
  }

  @Test(expected = RecipeException.class)
  public void testInvalidDateFunction() throws Exception {
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
    List<Row> rows = Arrays.asList(
      new Row("date", null).add("seconds", 86401).add("other", "2017-02-03T21:06:44Z")
    );

    rows = TestingRig.execute(directives, rows);
  }

  @Test
  public void testMethodCalls() throws Exception {
    String[] directives = new String[] {
      "set-column first first.trim()",
      "filter-row-if-true first.isEmpty()"
    };

    List<Row> rows = Arrays.asList(
      new Row("number", Bytes.toBytes(99))
        .add("first", "  ")
        .add("last", "joltie")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 0);
  }

  @Test
  public void testGeoFence() throws Exception {

    String geoJsonFence = "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{}," +
        "\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-122.05870628356934,37.37943348292772]," +
        "[-122.05724716186525,37.374727268782294],[-122.04634666442871,37.37493189292912]," +
        "[-122.04608917236328,37.38175237839049],[-122.05870628356934,37.37943348292772]]]}}]}";

    String[] directives = new String[]{
        "set column result geo:InFence(lat,lon,fences)"
    };

    List<Row> rows = Arrays.asList(
        new Row("id", 123)
            .add("lon", -462.49145507812494)
            .add("lat", 43.46089378008257)
            .add("fences", geoJsonFence)
    );
    rows = TestingRig.execute(directives, rows);
    Assert.assertFalse((Boolean) rows.get(0).getValue("result"));
  }

  @Test(expected = RecipeException.class)
  public void testMalformedGeoFence() throws Exception {
    String geoJsonFence = "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"properties\":{}," +
        "\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[-122.05870628356934,37.37943348292772]," +
        "[-122.05724716186525,37.374727268782294],[-122.04634666442871,37.37493189292912]," +
        "[-122.04608917236328,37.38175237839049]]]}}]}";

    String[] directives = new String[]{
        "set column result geo:InFence(lat,lon,fences)"
    };

    List<Row> rows = Arrays.asList(
        new Row("id", 123)
            .add("lon", -462.49145507812494)
            .add("lat", 43.46089378008257)
            .add("fences", geoJsonFence)
    );
    TestingRig.execute(directives, rows);
  }
}

