package co.cask.wrangler.steps.transformation.functions;

import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link DataQuality}
 */
public class DataQualityTest {

  @Test
  public void testRecordLength() throws Exception {
    Row row = new Row("a", 1).add("b", 2).add("c", 3);
    Assert.assertEquals(3, DataQuality.columns(row));
    row = new Row("a", 1);
    Assert.assertEquals(1, DataQuality.columns(row));
    row = new Row();
    Assert.assertEquals(0, DataQuality.columns(row));
  }

  @Test
  public void testRecordHasColumn() throws Exception {
    Row row = new Row("a", 1);
    Assert.assertEquals(true, DataQuality.hascolumn(row, "a"));
    Assert.assertEquals(false, DataQuality.hascolumn(row, "b"));
    row = new Row();
    Assert.assertEquals(false, DataQuality.hascolumn(row, "a"));
  }

  @Test
  public void testRange() throws Exception {
    Assert.assertEquals(true, DataQuality.inrange(1, 0, 10));
    Assert.assertEquals(false, DataQuality.inrange(0.9, 1, 10));
    Assert.assertEquals(true, DataQuality.inrange(1.1, 1, 10));
  }

}