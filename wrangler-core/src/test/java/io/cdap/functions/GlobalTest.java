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
import java.util.Collections;
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

  @Test
  public void testIsNotNull() throws Exception {
    String[] directives = new String[]{
      "set-column test1 IsNotNull(a) ? a : null",
      "set-column test2 IsNotNull(b) ? b : null",
      "set-column test3 IsNotNull(c) ? c : null",
      "set-column test4 if(IsNotNull(c)){ a } else {b}"
    };
    List<Row> rows = Collections.singletonList(new Row("a", null)
                                                 .add("b", "value")
                                                 .add("c", 999L)
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertNull(rows.get(0).getValue("test1"));
    Assert.assertEquals("value", rows.get(0).getValue("test2"));
    Assert.assertEquals(999L, rows.get(0).getValue("test3"));
    Assert.assertNull(rows.get(0).getValue("test4"));
  }

  @Test
  public void testIsNull() throws Exception {
    String[] directives = new String[]{
      "set-column test1 IsNull(a) ? a : null",
      "set-column test2 IsNull(b) ? b : null",
      "set-column test3 IsNull(c) ? c : null",
      "set-column test4 if(IsNull(c)){ a } else {b}"
    };
    List<Row> rows = Collections.singletonList(new Row("a", null)
                                                 .add("b", "value")
                                                 .add("c", 999L)
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertNull(rows.get(0).getValue("test1"));
    Assert.assertNull(rows.get(0).getValue("test2"));
    Assert.assertNull(rows.get(0).getValue("test3"));
    Assert.assertEquals("value", rows.get(0).getValue("test4"));
  }

  @Test
  public void testNullToEmpty() throws Exception {
    String[] directives = new String[]{
      "set-column test1 NullToEmpty(a)",
      "set-column test2 NullToEmpty(b)",
      "set-column test3 NullToEmpty(c)"
    };
    List<Row> rows = Collections.singletonList(new Row("a", null)
                                                 .add("b", "value")
                                                 .add("c", 999L)
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("", rows.get(0).getValue("test1"));
    Assert.assertEquals("value", rows.get(0).getValue("test2"));
    Assert.assertEquals(999L, rows.get(0).getValue("test3"));
  }

  @Test
  public void testNullToZero() throws Exception {
    String[] directives = new String[]{
      "set-column test1 NullToZero(a)",
      "set-column test2 NullToZero(b == 'value' ? a : b)",
      "set-column test3 NullToZero(c)"
    };
    List<Row> rows = Collections.singletonList(new Row("a", null)
                                                 .add("b", "value")
                                                 .add("c", 999L)
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(0, rows.get(0).getValue("test1"));
    Assert.assertEquals(0, rows.get(0).getValue("test2"));
    Assert.assertEquals(999L, rows.get(0).getValue("test3"));
  }

  @Test
  public void testNullToValue() throws Exception {
    String[] directives = new String[]{
      "set-column test1 NullToValue(a, 42)",
      "set-column test2 NullToValue(b == 'value' ? a : b, 42)",
      "set-column test3 NullToValue(c, 42)"
    };
    List<Row> rows = Collections.singletonList(new Row("a", null)
                                                 .add("b", "value")
                                                 .add("c", 999L)
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(42, rows.get(0).getValue("test1"));
    Assert.assertEquals(42, rows.get(0).getValue("test2"));
    Assert.assertEquals(999L, rows.get(0).getValue("test3"));
  }
}
