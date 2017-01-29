package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Copy}
 */
public class CopyTest {

  @Test
  public void testBasicCopy() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "copy body_1 name"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "A,B,C"),
      new Record("body", "D,E,F"),
      new Record("body", "G,H,I")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals(5, records.get(0).length()); // should have copied to another column
    Assert.assertEquals("A", records.get(0).getValue("name")); // Should have copy of 'A'
    Assert.assertEquals("D", records.get(1).getValue("name")); // Should have copy of 'D'
    Assert.assertEquals("G", records.get(2).getValue("name")); // Should have copy of 'G'
    Assert.assertEquals(records.get(0).getValue("name"), records.get(0).getValue("body_1"));
    Assert.assertEquals(records.get(1).getValue("name"), records.get(1).getValue("body_1"));
    Assert.assertEquals(records.get(2).getValue("name"), records.get(2).getValue("body_1"));
  }

  @Test(expected = StepException.class)
  public void testCopyToExistingColumn() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "copy body_1 body_2"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "A,B,C"),
      new Record("body", "D,E,F"),
      new Record("body", "G,H,I")
    );

    records = PipelineTest.execute(directives, records);
  }

  @Test
  public void testForceCopy() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body , true",
      "copy body_1 body_2 true"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "A,B,C"),
      new Record("body", "D,E,F"),
      new Record("body", "G,H,I")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals(4, records.get(0).length()); // should have copied to another column
    Assert.assertEquals("A", records.get(0).getValue("body_2")); // Should have copy of 'A'
    Assert.assertEquals("D", records.get(1).getValue("body_2")); // Should have copy of 'D'
    Assert.assertEquals("G", records.get(2).getValue("body_2")); // Should have copy of 'G'
    Assert.assertEquals(records.get(0).getValue("body_2"), records.get(0).getValue("body_1"));
    Assert.assertEquals(records.get(1).getValue("body_2"), records.get(1).getValue("body_1"));
    Assert.assertEquals(records.get(2).getValue("body_2"), records.get(2).getValue("body_1"));
  }

}