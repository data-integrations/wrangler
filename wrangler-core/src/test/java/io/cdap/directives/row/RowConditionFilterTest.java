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
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link RecordConditionFilter}
 */
public class RowConditionFilterTest {

  @Test(expected = RecipeException.class)
  public void testRHSLHSTypeDisconnect() throws Exception {
    String[] directives = new String[]{
      "parse-as-csv body ,",
      "drop body",
      "set columns PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked",
      "filter-row-if-true Fare < 10" // RHS is double, but it's not converted. Check next test case.
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "1,0,3,\"Braund, Mr. Owen Harris\",male,22,1,0,A/5 21171,7.25,,S"),
      new Row("body", "2,1,1,\"Cumings, Mrs. John Bradley (Florence Briggs Thayer)\",female," +
        "38,1,0,PC 17599,71.2833,C85,C")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
  }

  @Test
  public void testSameRHSAndLHSType() throws Exception {
    String[] directives = new String[]{
      "parse-as-csv body ,",
      "drop body",
      "set columns PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked",
      "filter-row-if-true Fare < 10.0" // RHS is changed to double, so LHS will also be changed.
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "1,0,3,\"Braund, Mr. Owen Harris\",male,22,1,0,A/5 21171,7.25,,S"),
      new Row("body", "2,1,1,\"Cumings, Mrs. John Bradley (Florence Briggs Thayer)\",female," +
        "38,1,0,PC 17599,71.2833,C85,C")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
  }

  @Test
  public void testComparingColumns() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ',' false",
      "drop body",
      "set columns id,fname,lname",
      "filter-row exp: { fname == lname} true"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "1,root,joltie"),
      new Row("body", "1,root,root"),
      new Row("body", "1,joltie,joltie")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("root", rows.get(0).getValue("fname"));
    Assert.assertEquals("joltie", rows.get(0).getValue("lname"));
  }
}
