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
      "set-column double { a = 43.2; number:AsDouble(a) }",
    };

    List<Row> rows = Arrays.asList(
      new Row()
    );
    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
  }
}
