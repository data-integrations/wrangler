package io.cdap.wrangler;

import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {
  @Test
  public void testTimeConversions() {
    Assert.assertEquals(100, new TimeDuration("100ms").getMilliseconds());
    Assert.assertEquals(2000, new TimeDuration("2s").getMilliseconds());
    Assert.assertEquals(180000, new TimeDuration("3m").getMilliseconds());
  }
}