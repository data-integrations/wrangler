package io.cdap.wrangler;

import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

  @Test
  public void testTimeConversions() {
    Assert.assertEquals(500, new TimeDuration("500ms").getMilliseconds());
    Assert.assertEquals(1500, new TimeDuration("1.5s").getMilliseconds());
    Assert.assertEquals(60000, new TimeDuration("60s").getMilliseconds());
  }
}
