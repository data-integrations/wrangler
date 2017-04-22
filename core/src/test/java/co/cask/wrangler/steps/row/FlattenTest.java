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

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
import org.json.JSONArray;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Flatten}
 */
public class FlattenTest {

  /**
   * { col1, col2, col3}
   * { col1=A }
   * { col1=B }
   * { col2=JSONArray{x1,y1}, col3=10}
   * { col2=JSONArray{x2,y2}, col3=11}
   *
   * Generates 6 rows
   *
   * { A, null, null}
   * { B, null, null}
   * { null, X1, 10}
   * { null, Y1, 10}
   * { null, Y2, 11}
   * { null, Y2, 11}
   *
   * @throws Exception
   */
  @Test
  public void testBasicCase1() throws Exception {
    String[] directives = new String[] {
      "flatten col1,col2,col3",
    };

    JSONArray a1 = new JSONArray(); a1.put("x1");a1.put("y1");
    JSONArray a2 = new JSONArray(); a2.put("x2");a2.put("y2");
    net.minidev.json.JSONArray b1 = new net.minidev.json.JSONArray(); b1.add("x1");b1.add("y1");
    net.minidev.json.JSONArray b2 = new net.minidev.json.JSONArray(); b2.add("x2");b2.add("y2");
    List<String> c1 = new ArrayList<>();c1.add("x1"); c1.add("y1");
    List<String> c2 = new ArrayList<>();c2.add("x2"); c2.add("y2");
    List<Record> records = Arrays.asList(
      new Record("col1", "A"),
      new Record("col1", "B"),
      new Record("col2", a1).add("col3", 10),
      new Record("col2", a2).add("col3", 11),
      new Record("col2", b1).add("col3", 10),
      new Record("col2", b2).add("col3", 11),
      new Record("col2", c1).add("col3", 10),
      new Record("col2", c2).add("col3", 11)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 14);
  }

  /**
   * { col1, col2, col3}
   * { col1=A }
   * { col1=B }
   * { col2=JSONArray{x1,y1,z1}, col3=10}
   * { col2=JSONArray{x2,y2}, col3=11}
   *
   * Generates 6 rows
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

    JSONArray a1 = new JSONArray(); a1.put("x1");a1.put("y1");a1.put("z1");
    JSONArray a2 = new JSONArray(); a2.put("x2");a2.put("y2");
    net.minidev.json.JSONArray b1 = new net.minidev.json.JSONArray(); b1.add("x1");b1.add("y1");b1.add("z1");
    net.minidev.json.JSONArray b2 = new net.minidev.json.JSONArray(); b2.add("x2");b2.add("y2");
    List<String> c1 = new ArrayList<>();c1.add("x1"); c1.add("y1"); c1.add("z1");
    List<String> c2 = new ArrayList<>();c2.add("x2"); c2.add("y2");
    List<Record> records = Arrays.asList(
      new Record("col1", "A"),
      new Record("col1", "B"),
      new Record("col2", a1).add("col3", 10),
      new Record("col2", a2).add("col3", 11),
      new Record("col2", b1).add("col3", 10),
      new Record("col2", b2).add("col3", 11),
      new Record("col2", c1).add("col3", 10),
      new Record("col2", c2).add("col3", 11)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 17);
  }

  /**
   * { col1, col2, col3}
   * { col1=A }
   * { col1=B }
   * { col2=JSONArray{x1,y1,z1}, col3={a1,b1,c1}}
   * { col2=JSONArray{x2,y2}, col3={a2,b2}}
   *
   * Generates 6 rows
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

    JSONArray a1 = new JSONArray(); a1.put("x1");a1.put("y1");a1.put("z1");
    JSONArray a2 = new JSONArray(); a2.put("x2");a2.put("y2");
    JSONArray a3 = new JSONArray(); a3.put("a1");a3.put("b1");a3.put("c1");
    JSONArray a4 = new JSONArray(); a4.put("a2");a4.put("b2");
    net.minidev.json.JSONArray b1 = new net.minidev.json.JSONArray(); b1.add("x1");b1.add("y1");b1.add("z1");
    net.minidev.json.JSONArray b2 = new net.minidev.json.JSONArray(); b2.add("x2");b2.add("y2");
    net.minidev.json.JSONArray b3 = new net.minidev.json.JSONArray(); b3.add("a1");b3.add("b1");b3.add("c1");
    net.minidev.json.JSONArray b4 = new net.minidev.json.JSONArray(); b4.add("a2");b4.add("b2");
    List<String> c1 = new ArrayList<>();c1.add("x1"); c1.add("y1"); c1.add("z1");
    List<String> c2 = new ArrayList<>();c2.add("x2"); c2.add("y2");
    List<String> c3 = new ArrayList<>();c3.add("a1"); c3.add("b1"); c3.add("c1");
    List<String> c4 = new ArrayList<>();c4.add("a2"); c4.add("b2");
    List<Record> records = Arrays.asList(
      new Record("col1", "A"),
      new Record("col1", "B"),
      new Record("col2", a1).add("col3", a3),
      new Record("col2", a2).add("col3", a4),
      new Record("col2", b1).add("col3", b3),
      new Record("col2", b2).add("col3", b4),
      new Record("col2", c1).add("col3", c3),
      new Record("col2", c2).add("col3", c4)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 17);
  }

  /**
   * { col1, col2, col3}
   * { col1=A }
   * { col1=B }
   * { col2=JSONArray{x1,y1,z1}, col3={a1,b1}}
   * { col2=JSONArray{x2,y2}, col3={a2,b2,c2}}
   *
   * Generates 6 rows
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

    JSONArray a1 = new JSONArray(); a1.put("x1");a1.put("y1");a1.put("z1");
    JSONArray a2 = new JSONArray(); a2.put("x2");a2.put("y2");
    JSONArray a3 = new JSONArray(); a3.put("a1");a3.put("b1");
    JSONArray a4 = new JSONArray(); a4.put("a2");a4.put("b2");a4.put("c2");
    net.minidev.json.JSONArray b1 = new net.minidev.json.JSONArray(); b1.add("x1");b1.add("y1");b1.add("z1");
    net.minidev.json.JSONArray b2 = new net.minidev.json.JSONArray(); b2.add("x2");b2.add("y2");
    net.minidev.json.JSONArray b3 = new net.minidev.json.JSONArray(); b3.add("a1");b3.add("b1");
    net.minidev.json.JSONArray b4 = new net.minidev.json.JSONArray(); b4.add("a2");b4.add("b2");b4.add("c2");
    List<String> c1 = new ArrayList<>();c1.add("x1"); c1.add("y1"); c1.add("z1");
    List<String> c2 = new ArrayList<>();c2.add("x2"); c2.add("y2");
    List<String> c3 = new ArrayList<>();c3.add("a1"); c3.add("b1");
    List<String> c4 = new ArrayList<>();c4.add("a2"); c4.add("b2"); c4.add("c2");
    List<Record> records = Arrays.asList(
      new Record("col1", "A"),
      new Record("col1", "B"),
      new Record("col2", a1).add("col3", a3),
      new Record("col2", a2).add("col3", a4),
      new Record("col2", b1).add("col3", b3),
      new Record("col2", b2).add("col3", b4),
      new Record("col2", c1).add("col3", c3),
      new Record("col2", c2).add("col3", c4)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 20);
  }

  @Test
  public void testFlatteningBasic() throws Exception {
    String[] directives = new String[] {
      "flatten FirstName",
      "flatten LastName"
    };

    JSONArray fname = new JSONArray();
    fname.put("SADASD");
    fname.put("ROOT");

    JSONArray lname = new JSONArray();
    lname.put("SADASDMR");
    lname.put("JOLTIE");

    List<Record> records = Arrays.asList(
      new Record("FirstName", fname).add("LastName", lname)
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 4);
  }
}
