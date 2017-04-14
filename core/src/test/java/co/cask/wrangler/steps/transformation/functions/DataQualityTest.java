package co.cask.wrangler.steps.transformation.functions;

import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link DataQuality}
 */
public class DataQualityTest {

  @Test
  public void testRecordLength() throws Exception {
    Record record = new Record("a", 1).add("b", 2).add("c", 3);
    Assert.assertEquals(3, DataQuality.columns(record));
    record = new Record("a", 1);
    Assert.assertEquals(1, DataQuality.columns(record));
    record = new Record();
    Assert.assertEquals(0, DataQuality.columns(record));
  }

  @Test
  public void testRecordHasColumn() throws Exception {
    Record record = new Record("a", 1);
    Assert.assertEquals(true, DataQuality.hascolumn(record, "a"));
    Assert.assertEquals(false, DataQuality.hascolumn(record, "b"));
    record = new Record();
    Assert.assertEquals(false, DataQuality.hascolumn(record, "a"));
  }

  @Test
  public void testRange() throws Exception {
    Assert.assertEquals(true, DataQuality.inrange(1, 0, 10));
    Assert.assertEquals(false, DataQuality.inrange(0.9, 1, 10));
    Assert.assertEquals(true, DataQuality.inrange(1.1, 1, 10));
  }

}