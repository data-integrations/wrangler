package co.cask.wrangler.steps.row;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
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

    JsonArray a1 = new JsonArray(); a1.add(new JsonPrimitive("x1"));a1.add(new JsonPrimitive("y1"));
    JsonArray a2 = new JsonArray(); a2.add(new JsonPrimitive("x2"));a2.add(new JsonPrimitive("y2"));
    List<String> b1 = new ArrayList<>();b1.add("x1"); b1.add("y1");
    List<String> b2 = new ArrayList<>();b2.add("x2"); b2.add("y2");
    List<Record> records = Arrays.asList(
      new Record("col1", "A"),
      new Record("col1", "B"),
      new Record("col2", a1).add("col3", 10),
      new Record("col2", a2).add("col3", 11),
      new Record("col2", b1).add("col3", 10),
      new Record("col2", b2).add("col3", 11)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 10);
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
    List<String> b1 = new ArrayList<>();b1.add("x1"); b1.add("y1"); b1.add("z1");
    List<String> b2 = new ArrayList<>();b2.add("x2"); b2.add("y2");
    List<Record> records = Arrays.asList(
      new Record("col1", "A"),
      new Record("col1", "B"),
      new Record("col2", a1).add("col3", 10),
      new Record("col2", a2).add("col3", 11),
      new Record("col2", b1).add("col3", 10),
      new Record("col2", b2).add("col3", 11)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 12);
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
    List<String> b1 = new ArrayList<>();b1.add("x1"); b1.add("y1"); b1.add("z1");
    List<String> b2 = new ArrayList<>();b2.add("x2"); b2.add("y2");
    List<String> b3 = new ArrayList<>();b3.add("a1"); b3.add("b1"); b3.add("c1");
    List<String> b4 = new ArrayList<>();b4.add("a2"); b4.add("b2");
    List<Record> records = Arrays.asList(
      new Record("col1", "A"),
      new Record("col1", "B"),
      new Record("col2", a1).add("col3", a3),
      new Record("col2", a2).add("col3", a4),
      new Record("col2", b1).add("col3", b3),
      new Record("col2", b2).add("col3", b4)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 12);
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
    List<String> b1 = new ArrayList<>();b1.add("x1"); b1.add("y1"); b1.add("z1");
    List<String> b2 = new ArrayList<>();b2.add("x2"); b2.add("y2");
    List<String> b3 = new ArrayList<>();b3.add("a1"); b3.add("b1");
    List<String> b4 = new ArrayList<>();b4.add("a2"); b4.add("b2"); b4.add("c2");
    List<Record> records = Arrays.asList(
      new Record("col1", "A"),
      new Record("col1", "B"),
      new Record("col2", a1).add("col3", a3),
      new Record("col2", a2).add("col3", a4),
      new Record("col2", b1).add("col3", b3),
      new Record("col2", b2).add("col3", b4)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 14);
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

    List<Record> records = Arrays.asList(
      new Record("FirstName", fname).add("LastName", lname)
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 4);
  }
}
