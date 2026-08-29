package io.cdap.wrangler.parser;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType; 


public class TimeDurationTest {

  @Test
  void testValidDurations() {
    assertEquals(1_000_000_000L, new TimeDuration("1s").getNanoseconds());
    assertEquals(60_000_000_000L, new TimeDuration("1min").getNanoseconds());
    assertEquals(3_600_000_000_000L, new TimeDuration("1h").getNanoseconds());
    assertEquals(1_000_000L, new TimeDuration("1ms").getNanoseconds());
    assertEquals(1_000L, new TimeDuration("1us").getNanoseconds());
    assertEquals(1L, new TimeDuration("1ns").getNanoseconds());
    assertEquals(90_000_000_000L, new TimeDuration("1.5min").getNanoseconds());
  }

  @Test
  void testAlternativeUnitNames() {
    assertEquals(1_000_000_000L, new TimeDuration("1 sec").getNanoseconds());
    assertEquals(1_000_000_000L, new TimeDuration("1 seconds").getNanoseconds());
    assertEquals(60_000_000_000L, new TimeDuration("1 minutes").getNanoseconds());
    assertEquals(3_600_000_000_000L, new TimeDuration("1 hrs").getNanoseconds());
  }

  @Test
  void testCaseInsensitivity() {
    assertEquals(1_000_000_000L, new TimeDuration("1 S").getNanoseconds());
    assertEquals(60_000_000_000L, new TimeDuration("1 MIN").getNanoseconds());
  }

  @Test
  void testInvalidDurations() {
    assertThrows(IllegalArgumentException.class, () -> new TimeDuration("10"));
    assertThrows(IllegalArgumentException.class, () -> new TimeDuration("abc ms"));
    assertThrows(IllegalArgumentException.class, () -> new TimeDuration("5 zz"));
  }

  @Test
  void testWhitespaceHandling() {
    assertEquals(2_000_000_000L, new TimeDuration("  2   s ").getNanoseconds());
  }

  @Test
  void testToJson() {
    TimeDuration duration = new TimeDuration("2s");
    assertEquals("2000000000", duration.toJson().getAsString());
  }

  @Test
  void testType() {
    TimeDuration duration = new TimeDuration("1min");
    assertEquals(TokenType.TIME_DURATION, duration.type());
  }

  @Test
  void testValueMethod() {
    TimeDuration duration = new TimeDuration("3 ms");
    assertEquals(3_000_000L, duration.value());
  }
}