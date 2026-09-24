package io.cdap.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {
  @Test
  public void testMilliseconds() {
    TimeDuration t = new TimeDuration("500ms");
    Assert.assertEquals(500L, t.getMilliseconds());
  }

  @Test
  public void testSeconds() {
    TimeDuration t = new TimeDuration("2.1s");
    Assert.assertEquals(2100L, t.getMilliseconds());
  }

  @Test
  public void testMinutes() {
    TimeDuration t = new TimeDuration("3min");
    Assert.assertEquals(180000L, t.getMilliseconds());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalid() {
    new TimeDuration("10xyz");
  }
}
