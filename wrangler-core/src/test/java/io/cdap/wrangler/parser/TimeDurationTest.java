package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimeDurationTest {
  @Test
  public void testTimeDurationParsing() {
    assertEquals(1_000_000, new TimeDuration("1ms").getNanoseconds());
    assertEquals(1_000_000_000, new TimeDuration("1s").getNanoseconds());
    assertEquals(60_000_000_000L, new TimeDuration("1m").getNanoseconds());
    assertEquals(2.5 * 1_000_000_000, new TimeDuration("2.5s").getNanoseconds(), 0.001);
  }
}
