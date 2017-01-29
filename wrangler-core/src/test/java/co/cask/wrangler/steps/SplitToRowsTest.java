package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SplitToRows}
 */
public class SplitToRowsTest {

  @Test
  public void testSplitToRows() throws Exception {
    String[] directives = new String[] {
      "split-to-rows body \\n",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "AABBCDE\nEEFFFF")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 2);
    Assert.assertEquals("AABBCDE", records.get(0).getValue("body"));
    Assert.assertEquals("EEFFFF", records.get(1).getValue("body"));
  }

  @Test
  public void testSplitWhenNoPatternMatch() throws Exception {
    String[] directives = new String[] {
      "split-to-rows body X",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "AABBCDE\nEEFFFF")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
  }

}