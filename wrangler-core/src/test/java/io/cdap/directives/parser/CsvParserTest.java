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

package io.cdap.directives.parser;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link CsvParser}
 */
public class CsvParserTest {
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

    List<Row> rows = Arrays.asList(
      new Row("body", "07/29/2013,Debt collection,\"Other (i.e. phone, health club, etc.)\",Cont'd attempts collect " +
        "debt not owed,Debt is not mine,,,\"NRA Group, LLC\",VA,20147,,N/A,Web,08/07/2013,Closed with non-monetary " +
        "relief,Yes,No,467801"),
      new Row("body", "07/29/2013,Mortgage,Conventional fixed mortgage,\"Loan servicing, payments, escrow account\",," +
        ",,Franklin Credit Management,CT,06106,,N/A,Web,07/30/2013,Closed with explanation,Yes,No,475823")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals("07/29/2013", rows.get(0).getValue("date"));
  }

  @Test
  public void testHeaders() throws Exception {
    String[] directives = new String[] { "parse-as-csv body , true" };

    List<Row> rows = Arrays.asList(
      new Row("body", "first name, last  \t  name"),
      new Row("body", "alice,zed")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("alice", rows.get(0).getValue("first_name"));
    Assert.assertEquals("zed", rows.get(0).getValue("last_name"));
  }

  @Test
  public void testTrailingCommas() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , false",
      "filter-rows-on regex-match body_1 ^school_id$",
      "drop body",
      "set columns school_id, student_id, last_name, first_name",
      "keep school_id,student_id,last_name,first_name"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "school_id, student_id, last_name, first_name,,,"),
      new Row("body", "14J456,33445566,Potter,Harry,,,"),
      new Row("body", "14J456,44333433,Weasley,Ron,,,"),
      new Row("body", "14J456,65765566,Granger,Hermione,,,"),
      new Row("body", "14J456,13233121,Diggory,Cedric,,,"),
      new Row("body", "14J456,98786868,Weasley,George,,,"),
      new Row("body", "14J456,78977876,Weasley,Fred,,,")
    );

    List<Row> expected = Arrays.asList(
      new Row("school_id", "14J456").add("student_id", "33445566").add("last_name", "Potter")
        .add("first_name", "Harry"),
      new Row("school_id", "14J456").add("student_id", "44333433").add("last_name", "Weasley")
        .add("first_name", "Ron"),
      new Row("school_id", "14J456").add("student_id", "65765566").add("last_name", "Granger")
        .add("first_name", "Hermione"),
      new Row("school_id", "14J456").add("student_id", "13233121").add("last_name", "Diggory")
        .add("first_name", "Cedric"),
      new Row("school_id", "14J456").add("student_id", "98786868").add("last_name", "Weasley")
        .add("first_name", "George"),
      new Row("school_id", "14J456").add("student_id", "78977876").add("last_name", "Weasley")
        .add("first_name", "Fred")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(6, rows.size());
    for (int i = 0; i < rows.size(); ++i) {
      Assert.assertEquals(4, rows.get(i).width());
      for (int j = 0; j < 4; j++) {
        Assert.assertEquals(expected.get(i).getValue(j), rows.get(i).getValue(j));
      }
    }
  }

  @Test
  public void testExtraCommasAndLeadingZeros() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , false",
      "filter-rows-on regex-match body_1 ^school_id$",
      "drop body",
      "set columns school_id, student_id, last_name, first_name, body_5",
      "set-column :last_name exp:{ this.width() == 5 ? (last_name + ',' + first_name) : last_name}",
      "set-column :first_name exp:{ this.width() == 5 ? body_5 : first_name}",
      "drop body_5"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "school_id, student_id, last_name, first_name"),
      new Row("body", "14J456,0033445566,Potter,Jr,Harry"),
      new Row("body", "14J456,0044333433,Weasley,Ron")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals(4, rows.get(0).width());
    Assert.assertEquals(4, rows.get(1).width());
    Assert.assertEquals("Potter,Jr", rows.get(0).getValue("last_name"));
    Assert.assertEquals("Harry", rows.get(0).getValue("first_name"));
    Assert.assertEquals("Weasley", rows.get(1).getValue("last_name"));
    Assert.assertEquals("Ron", rows.get(1).getValue("first_name"));
    Assert.assertEquals("0033445566", rows.get(0).getValue("student_id"));
    Assert.assertEquals("0044333433", rows.get(1).getValue("student_id"));
  }
}
