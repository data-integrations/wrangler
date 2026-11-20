package io.cdap.wrangler.parser;


import com.google.gson.JsonObject;

import io.cdap.wrangler.api.parser.TimeDuration;

import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

  @Test
  public void testValidDurations() {
    Assert.assertEquals(1_000_000, new TimeDuration("1ms").value().longValue());
    Assert.assertEquals(1_000_000_000, new TimeDuration("1s").value().longValue());
    Assert.assertEquals(60_000_000_000L, new TimeDuration("1min").value().longValue());
    Assert.assertEquals(3_600_000_000_000L, new TimeDuration("1h").value().longValue());
    Assert.assertEquals(86_400_000_000_000L, new TimeDuration("1d").value().longValue());
    Assert.assertEquals(500_000, new TimeDuration("0.5ms").value().longValue());
  }

  @Test
  public void testDefaultToMilliseconds() {
    Assert.assertEquals(1_500_000, new TimeDuration("1.5").value().longValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    new TimeDuration("ms1");
  }

  @Test
  public void testToJson() {
    TimeDuration duration = new TimeDuration("1s");
    JsonObject json = duration.toJson().getAsJsonObject();
    Assert.assertEquals("TIME_DURATION", json.get("type").getAsString());
    Assert.assertEquals(1_000_000_000, json.get("value").getAsLong());
    Assert.assertEquals("1s", json.get("original").getAsString());
  }
}