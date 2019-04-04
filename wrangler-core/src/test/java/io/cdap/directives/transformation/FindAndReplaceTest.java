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

package io.cdap.directives.transformation;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link FindAndReplace}
 */
public class FindAndReplaceTest {
  @Test
  public void testSedGrep() throws Exception {
    String[] directives = new String[] {
      "find-and-replace body s/\"//g"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "07/29/2013,Debt collection,\"Other (i.e. phone, health club, etc.)\",Cont'd attempts collect " +
        "debt not owed,Debt is not mine,,,\"NRA Group, LLC\",VA,20147,,N/A,Web,08/07/2013,Closed with non-monetary " +
        "relief,Yes,No,467801"),
      new Row("body", "07/29/2013,Mortgage,Conventional fixed mortgage,\"Loan servicing, payments, escrow account\",," +
        ",,Franklin Credit Management,CT,06106,,N/A,Web,07/30/2013,Closed with explanation,Yes,No,475823")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 2);
    Assert.assertEquals("07/29/2013,Debt collection,Other (i.e. phone, health club, etc.),Cont'd " +
                          "attempts collect debt not owed,Debt is not mine,,,NRA Group, LLC,VA,20147,,N/A," +
                          "Web,08/07/2013,Closed with non-monetary relief,Yes,No,467801",
                        rows.get(0).getValue("body"));
  }

}
