package io.cdap.wrangler.api.parser;

import static org.junit.Assert.*;

import org.junit.Test;

public class ByteSizeAndTimeDurationTest {

  @Test
  public void testByteSizeParsing() {
    // Test various byte size formats
    ByteSize size1 = new ByteSize("10B");
    assertEquals(10L, ((Number)size1.value()).longValue());

    ByteSize size2 = new ByteSize("1.5KB");
    assertEquals(1536L, ((Number)size2.value()).longValue());

    ByteSize size3 = new ByteSize("2MB");
    assertEquals(2 * 1024 * 1024L, ((Number)size3.value()).longValue());

    ByteSize size4 = new ByteSize("1.25GB");
    assertEquals((long)(1.25 * 1024 * 1024 * 1024), ((Number)size4.value()).longValue());

    ByteSize size5 = new ByteSize("0.5TB");
    assertEquals((long)(0.5 * 1024 * 1024 * 1024 * 1024), ((Number)size5.value()).longValue());
  }

  @Test
  public void testTimeDurationParsing() {
    // Test various time duration formats
    TimeDuration time1 = new TimeDuration("100ns");
    assertEquals(100L, ((Number)time1.value()).longValue());

    TimeDuration time2 = new TimeDuration("1.5ms");
    assertEquals(1_500_000L, ((Number)time2.value()).longValue());

    TimeDuration time3 = new TimeDuration("2s");
    assertEquals(2_000_000_000L, ((Number)time3.value()).longValue());

    TimeDuration time4 = new TimeDuration("1.5m");
    assertEquals((long)(1.5 * 60 * 1_000_000_000), ((Number)time4.value()).longValue());

    TimeDuration time5 = new TimeDuration("0.5h");
    assertEquals((long)(0.5 * 3600 * 1_000_000_000), ((Number)time5.value()).longValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidByteSize() {
    new ByteSize("invalid");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeDuration() {
    new TimeDuration("invalid");
  }

  @Test
  public void testByteSizeEdgeCases() {
    // Test edge cases for byte sizes
    ByteSize size1 = new ByteSize("0B");
    assertEquals(0L, ((Number)size1.value()).longValue());

    ByteSize size2 = new ByteSize("1024B");
    assertEquals(1024L, ((Number)size2.value()).longValue());

    ByteSize size3 = new ByteSize("1KB");
    assertEquals(1024L, ((Number)size3.value()).longValue());
  }

  @Test
  public void testTimeDurationEdgeCases() {
    // Test edge cases for time durations
    TimeDuration time1 = new TimeDuration("0ns");
    assertEquals(0L, ((Number)time1.value()).longValue());

    TimeDuration time2 = new TimeDuration("1000ns");
    assertEquals(1000L, ((Number)time2.value()).longValue());

    TimeDuration time3 = new TimeDuration("1ms");
    assertEquals(1_000_000L, ((Number)time3.value()).longValue());
  }
} 