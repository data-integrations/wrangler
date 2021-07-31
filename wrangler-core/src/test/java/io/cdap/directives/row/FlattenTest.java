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

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests {@link Flatten}
 */
public class FlattenTest {

  @Test
  public void testEmptyList() throws Exception {
    String[] directives = new String[] {
      "flatten x"
    };

    Row row = new Row("x", new ArrayList<>()).add("y", "y");
    List<Row> output = TestingRig.execute(directives, Collections.singletonList(row));

    Assert.assertEquals(1, output.size());
    Assert.assertEquals("y", output.get(0).getValue("y"));
    Assert.assertNull(((List) output.get(0).getValue("x")));
  }

  /**
   * { col1, col2, col3}
   * { col1=A }
   * { col1=B }
   * { col2=JsonArray{x1,y1}, col3=10}
   * { col2=JsonArray{x2,y2}, col3=11}
   *
   * Generates 10 rows
   *
   * { A, null, null}
   * { B, null, null}
   * { null, X1, 10}
   * { null, Y1, 10}
   * { null, X2, 11}
   * { null, Y2, 11}
   *
   * @throws Exception
   */
  @Test
  public void testBasicCase1() throws Exception {
    String[] directives = new String[] {
      "flatten col1,col2,col3",
    };

    JsonArray a1 = new JsonArray();
    a1.add(new JsonPrimitive("x1"));
    a1.add(new JsonPrimitive("y1"));
    JsonArray a2 = new JsonArray();
    a2.add(new JsonPrimitive("x2"));
    a2.add(new JsonPrimitive("y2"));
    List<String> b1 = new ArrayList<>();
    b1.add("x1");
    b1.add("y1");
    List<String> b2 = new ArrayList<>();
    b2.add("x2");
    b2.add("y2");
    List<Row> rows = Arrays.asList(
      new Row("col1", "A"),
      new Row("col1", "B"),
      new Row("col2", a1).add("col3", 10),
      new Row("col2", a2).add("col3", 11),
      new Row("col2", b1).add("col3", 10),
      new Row("col2", b2).add("col3", 11)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 10);
  }

  /**
   * { col1, col2, col3}
   * { col1=A }
   * { col1=B }
   * { col2=JSONArray{x1,y1,z1}, col3=10}
   * { col2=JSONArray{x2,y2}, col3=11}
   *
   * Generates 12 rows
   *
   * { A, null, null}
   * { B, null, null}
   * { null, X1, 10}
   * { null, Y1, 10}
   * { null, Z1, 10}
   * { null, Y1, 11}
   * { null, Y2, 11}
   *
   * @throws Exception
   */
  @Test
  public void testBasicCase2() throws Exception {
    String[] directives = new String[] {
      "flatten col1,col2,col3",
    };

    JsonArray a1 = new JsonArray();
    a1.add(new JsonPrimitive("x1"));
    a1.add(new JsonPrimitive("y1"));
    a1.add(new JsonPrimitive("z1"));
    JsonArray a2 = new JsonArray();
    a2.add(new JsonPrimitive("x2"));
    a2.add(new JsonPrimitive("y2"));
    List<String> b1 = new ArrayList<>();
    b1.add("x1");
    b1.add("y1");
    b1.add("z1");
    List<String> b2 = new ArrayList<>();
    b2.add("x2");
    b2.add("y2");
    List<Row> rows = Arrays.asList(
      new Row("col1", "A"),
      new Row("col1", "B"),
      new Row("col2", a1).add("col3", 10),
      new Row("col2", a2).add("col3", 11),
      new Row("col2", b1).add("col3", 10),
      new Row("col2", b2).add("col3", 11)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 12);
  }

  /**
   * { col1, col2, col3}
   * { col1=A }
   * { col1=B }
   * { col2=JSONArray{x1,y1,z1}, col3={a1,b1,c1}}
   * { col2=JSONArray{x2,y2}, col3={a2,b2}}
   *
   * Generates 12 rows
   *
   * { A, null, null}
   * { B, null, null}
   * { null, X1, a1}
   * { null, Y1, b1}
   * { null, z1, c1}
   * { null, x2, a2}
   * { null, y2, b2}
   *
   * @throws Exception
   */
  @Test
  public void testBasicCase3() throws Exception {
    String[] directives = new String[] {
      "flatten col1,col2,col3",
    };

    JsonArray a1 = new JsonArray();
    a1.add(new JsonPrimitive("x1"));
    a1.add(new JsonPrimitive("y1"));
    a1.add(new JsonPrimitive("z1"));
    JsonArray a2 = new JsonArray();
    a2.add(new JsonPrimitive("x2"));
    a2.add(new JsonPrimitive("y2"));
    JsonArray a3 = new JsonArray();
    a3.add(new JsonPrimitive("a1"));
    a3.add(new JsonPrimitive("b1"));
    a3.add(new JsonPrimitive("c1"));
    JsonArray a4 = new JsonArray();
    a4.add(new JsonPrimitive("a2"));
    a4.add(new JsonPrimitive("b2"));
    List<String> b1 = new ArrayList<>();
    b1.add("x1");
    b1.add("y1");
    b1.add("z1");
    List<String> b2 = new ArrayList<>();
    b2.add("x2");
    b2.add("y2");
    List<String> b3 = new ArrayList<>();
    b3.add("a1");
    b3.add("b1");
    b3.add("c1");
    List<String> b4 = new ArrayList<>();
    b4.add("a2");
    b4.add("b2");
    List<Row> rows = Arrays.asList(
      new Row("col1", "A"),
      new Row("col1", "B"),
      new Row("col2", a1).add("col3", a3),
      new Row("col2", a2).add("col3", a4),
      new Row("col2", b1).add("col3", b3),
      new Row("col2", b2).add("col3", b4)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 12);
  }

  /**
   * { col1, col2, col3}
   * { col1=A }
   * { col1=B }
   * { col2=JSONArray{x1,y1,z1}, col3={a1,b1}}
   * { col2=JSONArray{x2,y2}, col3={a2,b2,c2}}
   *
   * Generates 14 rows
   *
   * { A, null, null}
   * { B, null, null}
   * { null, X1, a1}
   * { null, Y1, b1}
   * { null, z1, null}
   * { null, x2, a2}
   * { null, y2, b2}
   * { null, null, c2}
   *
   * @throws Exception
   */
  @Test
  public void testBasicCase4() throws Exception {
    String[] directives = new String[] {
      "flatten col1,col2,col3",
    };

    JsonArray a1 = new JsonArray();
    a1.add(new JsonPrimitive("x1"));
    a1.add(new JsonPrimitive("y1"));
    a1.add(new JsonPrimitive("z1"));
    JsonArray a2 = new JsonArray();
    a2.add(new JsonPrimitive("x2"));
    a2.add(new JsonPrimitive("y2"));
    JsonArray a3 = new JsonArray();
    a3.add(new JsonPrimitive("a1"));
    a3.add(new JsonPrimitive("b1"));
    JsonArray a4 = new JsonArray();
    a4.add(new JsonPrimitive("a2"));
    a4.add(new JsonPrimitive("b2"));
    a4.add(new JsonPrimitive("c2"));
    List<String> b1 = new ArrayList<>();
    b1.add("x1");
    b1.add("y1");
    b1.add("z1");
    List<String> b2 = new ArrayList<>();
    b2.add("x2");
    b2.add("y2");
    List<String> b3 = new ArrayList<>();
    b3.add("a1");
    b3.add("b1");
    List<String> b4 = new ArrayList<>();
    b4.add("a2");
    b4.add("b2");
    b4.add("c2");
    List<Row> rows = Arrays.asList(
      new Row("col1", "A"),
      new Row("col1", "B"),
      new Row("col2", a1).add("col3", a3),
      new Row("col2", a2).add("col3", a4),
      new Row("col2", b1).add("col3", b3),
      new Row("col2", b2).add("col3", b4)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 14);
  }

  @Test
  public void testFlatteningBasic() throws Exception {
    String[] directives = new String[] {
      "flatten FirstName",
      "flatten LastName"
    };

    JsonArray fname = new JsonArray();
    fname.add(new JsonPrimitive("SADASD"));
    fname.add(new JsonPrimitive("ROOT"));

    JsonArray lname = new JsonArray();
    lname.add(new JsonPrimitive("SADASDMR"));
    lname.add(new JsonPrimitive("JOLTIE"));

    List<Row> rows = Arrays.asList(
      new Row("FirstName", fname).add("LastName", lname)
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 4);
  }

  @Test(expected = DirectiveParseException.class)
  public void testSyntaxFailure1() throws Exception {
    String[] directives = new String[] {
      "flatten",
    };

    JsonArray fname = new JsonArray();
    fname.add(new JsonPrimitive("SADASD"));
    fname.add(new JsonPrimitive("ROOT"));

    JsonArray lname = new JsonArray();
    lname.add(new JsonPrimitive("SADASDMR"));
    lname.add(new JsonPrimitive("JOLTIE"));

    List<Row> rows = Arrays.asList(
      new Row("FirstName", fname).add("LastName", lname)
    );

    TestingRig.execute(directives, rows);
  }
}
