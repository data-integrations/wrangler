package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link FillNullOrEmpty}
 */
public class FillNullOrEmptyTest {

  @Test(expected = StepException.class)
  public void testColumnNotSpecified() throws Exception {
    String[] directives = new String[] {
      "fill-null-or-empty null N/A",
    };

    List<Record> records = Arrays.asList(
      new Record("value", "has value"),
      new Record("value", null),
      new Record("value", null)
    );

    PipelineTest.execute(directives, records);
  }

  @Test
  public void testBasicNullCase() throws Exception {
    String[] directives = new String[] {
      "fill-null-or-empty value N/A",
    };

    List<Record> records = Arrays.asList(
      new Record("value", "has value"),
      new Record("value", null),
      new Record("value", null)
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals("has value", records.get(0).getValue("value"));
    Assert.assertEquals("N/A", records.get(1).getValue("value"));
    Assert.assertEquals("N/A", records.get(2).getValue("value"));
  }

  @Test
  public void testEmptyStringCase() throws Exception {
    String[] directives = new String[] {
      "fill-null-or-empty value N/A",
    };

    List<Record> records = Arrays.asList(
      new Record("value", ""),
      new Record("value", ""),
      new Record("value", "Should be fine")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals("N/A", records.get(0).getValue("value"));
    Assert.assertEquals("N/A", records.get(1).getValue("value"));
    Assert.assertEquals("Should be fine", records.get(2).getValue("value"));
  }

  @Test
  public void testMixedCases() throws Exception {
    String[] directives = new String[] {
      "fill-null-or-empty value N/A",
    };

    List<Record> records = Arrays.asList(
      new Record("value", null),
      new Record("value", ""),
      new Record("value", "Should be fine")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals("N/A", records.get(0).getValue("value"));
    Assert.assertEquals("N/A", records.get(1).getValue("value"));
    Assert.assertEquals("Should be fine", records.get(2).getValue("value"));
  }
}