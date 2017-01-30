package co.cask.wrangler.steps;

import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link GenerateUUID}
 */
public class GenerateUUIDTest {

  @Test
  public void testUUIDGeneration() throws Exception {
    String[] directives = new String[] {
      "generate-uuid uuid",
    };

    List<Record> records = Arrays.asList(
      new Record("value", "abc"),
      new Record("value", "xyz"),
      new Record("value", "Should be fine")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals(2, records.get(0).length());
    Assert.assertEquals("uuid", records.get(1).getColumn(1));
    Assert.assertEquals("Should be fine", records.get(2).getValue("value"));
  }

}