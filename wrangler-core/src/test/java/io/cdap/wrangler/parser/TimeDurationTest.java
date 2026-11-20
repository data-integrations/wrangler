package io.cdap.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

import io.cdap.wrangler.api.parser.TimeDuration;

public class TimeDurationTest {

  @Test
  public void testTimeDurationParsing() {
    Assert.assertEquals(500L, new TimeDuration("500ms").getMilliseconds());
    Assert.assertEquals(1500L, new TimeDuration("1.5s").getMilliseconds());
    Assert.assertEquals(60000L, new TimeDuration("1m").getMilliseconds());
    Assert.assertEquals(3600000L, new TimeDuration("1h").getMilliseconds());
    Assert.assertEquals(86400000L, new TimeDuration("1d").getMilliseconds());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDuration() {
    new TimeDuration("12weeks");
  }
}
