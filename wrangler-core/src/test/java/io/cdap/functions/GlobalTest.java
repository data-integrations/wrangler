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

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Global}
 */
public class GlobalTest {

  @Test
  public void testFirstNonNullFound() throws Exception {
    String[] directives = new String[] {
      "set-column d coalesce(a,b,c)"
    };

    // Run through the wrangling steps.
    List<Row> rows = Arrays.asList(new Row("a", null).add("b", null).add("c", "c"));

    // Iterate through steps.
    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals("c", rows.get(0).getValue("d"));
  }

  @Test
  public void testFirstNonNullNotFound() throws Exception {
    String[] directives = new String[] {
      "set-column d coalesce(a,b,c)"
    };

    // Run through the wrangling steps.
    List<Row> rows = Arrays.asList(new Row("a", null).add("b", null).add("c", null));

    // Iterate through steps.
    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(null, rows.get(0).getValue("d"));
  }

  @Test
  public void testFirstNonNullFoundInBetween() throws Exception {
    String[] directives = new String[] {
      "set-column d coalesce(a,b,c)"
    };

    // Run through the wrangling steps.
    List<Row> rows = Arrays.asList(new Row("a", "a").add("b", null).add("c", "c"));

    // Iterate through steps.
    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals("a", rows.get(0).getValue("d"));
  }

  @Test
  public void testFirstNonNullFoundAtStart() throws Exception {
    String[] directives = new String[] {
      "set-column d coalesce(a,b,c)"
    };

    // Run through the wrangling steps.
    List<Row> rows = Arrays.asList(new Row("a", "a").add("b", null).add("c", null));

    // Iterate through steps.
    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals("a", rows.get(0).getValue("d"));
  }

  @Test
  public void testPrint() throws Exception {
    String[] directives = new String[] {
      "set-column d format(\"%s-%s-%s\", a,b,c)"
    };

    // Run through the wrangling steps.
    List<Row> rows = Arrays.asList(new Row("a", "a").add("b", "b").add("c", "c"));

    // Iterate through steps.
    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals("a-b-c", rows.get(0).getValue("d"));
  }
}
