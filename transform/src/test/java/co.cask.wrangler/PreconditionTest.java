package co.cask.wrangler;

import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Precondition}
 */
public class PreconditionTest {

  @Test
  public void testPrecondition() throws Exception {
    Record record = new Record("a", 1).add("b", "x").add("c", 2.06);
    Assert.assertEquals(true, new Precondition("a == 1 && b == \"x\"").apply(record));
    Assert.assertEquals(true, new Precondition("c > 2.0").apply(record));
    Assert.assertEquals(true, new Precondition("true").apply(record));
    Assert.assertEquals(false, new Precondition("false").apply(record));
  }

  @Test(expected = PreconditionException.class)
  public void testBadCondition() throws Exception {
    Record record = new Record("a", 1).add("b", "x").add("c", 2.06);
    Assert.assertEquals(true, new Precondition("c").apply(record));
  }
}