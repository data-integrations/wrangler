/*
 *  Copyright © 2020 Cask Data, Inc.
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

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests functionality of {@link NumberFunctions}
 */
public class NumberFunctionsTest {

  @Test
  public void testNumbers() throws Exception {
    String[] directives = new String[]{
      "set-column double { a = 5.0; number:AsDouble(a/2) }",
      "set-column float { a = 2.34; number:AsFloat(a) }",
      "set-column int { a = 5.64; number:AsInteger(a) }",
      "set-column mantissaInt { a = 5; number:Mantissa(a) }",
      "set-column mantissaDouble { a = 43.2534d; number:Mantissa(a) }",
      "set-column mantissaLong { a = 214748364721l; number:Mantissa(a) }",
      "set-column mantissaFloat { a = 12.0234; number:Mantissa(a) }",
      "set-column mantissaBigD { a = 12.00123b; number:Mantissa(a) }",
    };

    List<Row> rows = Arrays.asList(
      new Row()
    );
    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(2.5d, rows.get(0).getValue("double"));
    Assert.assertEquals(2.34f, rows.get(0).getValue("float"));
    Assert.assertEquals(5, rows.get(0).getValue("int"));
    Assert.assertEquals(0d, rows.get(0).getValue("mantissaInt"));
    Assert.assertEquals(0.2534d, rows.get(0).getValue("mantissaDouble"));
    Assert.assertEquals(0d, rows.get(0).getValue("mantissaLong"));
    Assert.assertEquals(0.0234d, rows.get(0).getValue("mantissaFloat"));
    Assert.assertEquals(0.00123d, rows.get(0).getValue("mantissaBigD"));
  }
}
