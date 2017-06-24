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

package co.cask.wrangler.steps.row;

import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.RecipePipelineTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link RecordConditionFilter}
 */
public class RecordConditionFilterTest {

  @Test
  public void testRowFilterRegex() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv __col ,",
      "drop __col",
      "set columns id,first,last,dob,email,age,hrlywage,address,city,state,country,zip",
      "filter-row-if-matched email NULL",
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

    records = RecipePipelineTest.execute(directives, records);

    // Filters all the records that don't match the pattern .*@joltie.io
    Assert.assertTrue(records.size() == 1);
  }

  @Test(expected = DirectiveExecutionException.class)
  public void testRHSLHSTypeDisconnect() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "drop body",
      "set columns PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked",
      "filter-row-if-true Fare < 10" // RHS is double, but it's not converted. Check next test case.
    };

    List<Record> records = Arrays.asList(
      new Record("body", "1,0,3,\"Braund, Mr. Owen Harris\",male,22,1,0,A/5 21171,7.25,,S"),
      new Record("body", "2,1,1,\"Cumings, Mrs. John Bradley (Florence Briggs Thayer)\",female," +
        "38,1,0,PC 17599,71.2833,C85,C")
    );

    records = RecipePipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
  }

  @Test
  public void testSameRHSAndLHSType() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "drop body",
      "set columns PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked",
      "filter-row-if-true Fare < 10.0" // RHS is changed to double, so LHS will also be changed.
    };

    List<Record> records = Arrays.asList(
      new Record("body", "1,0,3,\"Braund, Mr. Owen Harris\",male,22,1,0,A/5 21171,7.25,,S"),
      new Record("body", "2,1,1,\"Cumings, Mrs. John Bradley (Florence Briggs Thayer)\",female," +
        "38,1,0,PC 17599,71.2833,C85,C")
    );

    records = RecipePipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
  }

}