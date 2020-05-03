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

package io.cdap.directives.row;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link RecordRegexFilter}
 */
public class RowRegexFilterTest {

  @Test
  public void testRowFilterRegex() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv __col ,",
      "drop __col",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "filter-by-regex if-matched :email 'NULL'",
      "filter-by-regex if-matched :email '.*@joltie.io'",
      "filter-row-if-true id > 1092",
      "filter-rows-on regex-match last .*(?i)harris.*"
    };

    List<Row> rows = Arrays.asList(
      new Row("__col", "1098,Root,Joltie,01/26/1956,root@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1091,Root,Joltie,01/26/1956,root1@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1092,Root,Joltie,01/26/1956,root@mars.com,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1093,Root,Joltie,01/26/1956,root@foo.com,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1094,Super,Joltie,01/26/1956,windy@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826")
    );

    rows = TestingRig.execute(directives, rows);

    // Filters all the rows that don't match the pattern .*@joltie.io
    Assert.assertTrue(rows.size() == 1);
  }

  @Test
  public void testStarCondition() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv __col ,",
      "drop __col",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "filter-by-regex if-matched :email '.*root.*'"
    };

    List<Row> rows = Arrays.asList(
      new Row("__col", "1098,Root,Joltie,01/26/1956,root@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1091,Root,Joltie,01/26/1956,root1@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1092,Root,Joltie,01/26/1956,root@mars.com,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1093,Root,Joltie,01/26/1956,root@foo.com,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826"),
      new Row("__col", "1094,Super,Joltie,01/26/1956,windy@joltie.io,32,11.79,150 Mars Ave,Palo Alto,CA,USA,32826")
    );

    rows = TestingRig.execute(directives, rows);

    // Filters all the rows that don't match the pattern .*@joltie.io
    Assert.assertEquals(1, rows.size());
  }

  @Test
  public void testFilterOnNumericValues() throws Exception {
    List<String> directives = new ArrayList<>();

    directives.add("parse-as-csv __col ,");
    directives.add("drop __col");
    directives.add("set columns id,string,int,short,long,float,double");
    directives.add("set-type :int integer");
    directives.add("set-type :short short");
    directives.add("set-type :long long");
    directives.add("set-type :float float");
    directives.add("set-type :double double");
    directives.add("filter-rows-on condition-false int == 5005");

    List<Row> originalRows = Arrays.asList(
      new Row("__col", "1,san jose,1001,1,11,22.1,55.1"),
      new Row("__col", "2,palo alto,2002,2,22,22.2,55.2"),
      new Row("__col", "3,mountain view,3,3003,33,22.3,55.3"),
      new Row("__col", "4,saratoga,4004,4,44,22.4,55.4"),
      new Row("__col", "5,los altos,5005,5,55,22.5,55.5")
    );

    // test integer support
    List<Row> rows = TestingRig.execute(directives.toArray(new String[directives.size()]), originalRows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(5005, rows.get(0).getValue(2));

    // test short support
    directives.remove(8);
    directives.add("filter-rows-on regex-not-match long .*2.*");
    rows = TestingRig.execute(directives.toArray(new String[directives.size()]), originalRows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(new Short("2"), rows.get(0).getValue(3));

    // test long support
    directives.remove(8);
    directives.add("filter-rows-on regex-not-match long .*2.*");
    rows = TestingRig.execute(directives.toArray(new String[directives.size()]), originalRows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(22L, rows.get(0).getValue(4));

    // test float support
    directives.remove(8);
    directives.add("filter-rows-on regex-not-match float .*22.4.*");
    rows = TestingRig.execute(directives.toArray(new String[directives.size()]), originalRows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(22.4f, rows.get(0).getValue(5));

    // test double support
    directives.remove(8);
    directives.add("filter-rows-on regex-not-match double .*55.1.*");
    rows = TestingRig.execute(directives.toArray(new String[directives.size()]), originalRows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(55.1d, rows.get(0).getValue(6));

    // test string support
    directives.remove(8);
    directives.add("filter-rows-on regex-not-match string .*sar.*");
    rows = TestingRig.execute(directives.toArray(new String[directives.size()]), originalRows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("saratoga", rows.get(0).getValue(1));
  }

  @Test
  public void testFilterKeepDoesntKeepNullValues() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv :body ',' false",
      "filter-by-regex if-matched :body_3 '.*pot.*'"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "1, \"Archil\", , \"SHAH\", 19, \"2017-06-02\""),
      new Row("body", "2, \"Sameet\", \"andpotatoes\", \"Sapra\", 19, \"2017-06-02\""),
      new Row("body", "3, \"Bob\", , \"Sagett\", 101, \"1970-01-01\"")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 2);
  }
}
