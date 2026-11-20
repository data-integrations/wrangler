/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api.parser;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link TimeDuration} class.
 */
public class TimeDurationTest {

  @Test
  public void testValidTimeDurationConstructor() {
    TimeDuration duration = new TimeDuration("10ms");
    assertEquals("10ms", duration.value());
    assertEquals(TokenType.TIME_DURATION, duration.type());

    duration = new TimeDuration("1.5s");
    assertEquals("1.5s", duration.value());
    assertEquals(TokenType.TIME_DURATION, duration.type());
  }

  @Test
  public void testNanosecondsConversion() {
    // Test milliseconds (ms)
    assertEquals(10_000_000L, new TimeDuration("10ms").getNanoseconds());
    assertEquals(100_000_000L, new TimeDuration("100ms").getNanoseconds());

    // Test seconds (s)
    assertEquals(1_000_000_000L, new TimeDuration("1s").getNanoseconds());
    assertEquals(1_500_000_000L, new TimeDuration("1.5s").getNanoseconds());

    // Test minutes (m)
    assertEquals(60_000_000_000L, new TimeDuration("1m").getNanoseconds());
    assertEquals(120_000_000_000L, new TimeDuration("2m").getNanoseconds());

    // Test hours (h)
    assertEquals(3600_000_000_000L, new TimeDuration("1h").getNanoseconds());
    assertEquals(7200_000_000_000L, new TimeDuration("2h").getNanoseconds());

    // Test days (d)
    assertEquals(86400_000_000_000L, new TimeDuration("1d").getNanoseconds());
    assertEquals(172800_000_000_000L, new TimeDuration("2d").getNanoseconds());
  }

  @Test(expected = NumberFormatException.class)
  public void testEmptyString() {
    new TimeDuration("");
  }

  @Test(expected = NumberFormatException.class)
  public void testMissingNumber() {
    new TimeDuration("ms");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingUnit() {
    new TimeDuration("100").getNanoseconds();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    new TimeDuration("100xs").getNanoseconds();
  }

  @Test
  public void testEdgeCases() {
    // Test zero value
    assertEquals(0L, new TimeDuration("0ms").getNanoseconds());
    assertEquals(0L, new TimeDuration("0s").getNanoseconds());
    assertEquals(0L, new TimeDuration("0m").getNanoseconds());

    // Test decimal precision
    assertEquals(1_500_000_000L, new TimeDuration("1.5s").getNanoseconds());
    assertEquals(2_500_000_000L, new TimeDuration("2.5s").getNanoseconds());

    // Test fractional milliseconds
    assertEquals(1_500_000L, new TimeDuration("1.5ms").getNanoseconds());
  }

  @Test
  public void testToJson() {
    TimeDuration duration = new TimeDuration("1.5s");
    assertNotNull(duration.toJson());
    assertTrue(duration.toJson().isJsonObject());
    assertEquals("TIME_DURATION", duration.toJson().getAsJsonObject().get("type").getAsString());
    assertEquals("1.5s", duration.toJson().getAsJsonObject().get("value").getAsString());
  }
}