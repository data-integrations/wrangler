/*
 * Copyright © 2023-2025 Cask Data, Inc.
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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

/**
 * Tests for {@link TimeDuration} class
 */
public class TimeDurationTest {

  @Test
  public void testMilliseconds() throws Exception {
    TimeDuration duration = new TimeDuration("100ms");
    Assert.assertEquals(100 * 1_000_000L, duration.getNanoseconds());
    Assert.assertEquals(100.0, duration.getMilliseconds(), 0.001);
    Assert.assertEquals("100ms", duration.value());
    Assert.assertEquals(TokenType.TIME_DURATION, duration.type());
  }

  @Test
  public void testSeconds() throws Exception {
    TimeDuration duration = new TimeDuration("1.5s");
    Assert.assertEquals(1.5 * 1_000_000_000L, duration.getNanoseconds(), 1.0); // Allow for small rounding differences
    Assert.assertEquals(1.5, duration.getSeconds(), 0.001);
  }

  @Test
  public void testMinutes() throws Exception {
    TimeDuration duration = new TimeDuration("2min");
    long expectedNanos = 2L * 60 * 1_000_000_000L;
    Assert.assertEquals(expectedNanos, duration.getNanoseconds());
    Assert.assertEquals(2.0, duration.getMinutes(), 0.001);
  }

  @Test
  public void testHours() throws Exception {
    TimeDuration duration = new TimeDuration("1h");
    long expectedNanos = 60L * 60 * 1_000_000_000L;
    Assert.assertEquals(expectedNanos, duration.getNanoseconds());
    Assert.assertEquals(1.0, duration.getHours(), 0.001);
  }

  @Test
  public void testUnitConversions() throws Exception {
    TimeDuration oneSecond = new TimeDuration("1s");
    Assert.assertEquals(1.0, oneSecond.getSeconds(), 0.001);
    Assert.assertEquals(1000.0, oneSecond.getMilliseconds(), 0.001);
    Assert.assertEquals(1_000_000.0, oneSecond.getMicroseconds(), 0.001);
    Assert.assertEquals(1.0 / 60, oneSecond.getMinutes(), 0.001);
  }

  @Test(expected = ParseException.class)
  public void testInvalidFormat() throws Exception {
    new TimeDuration("10seconds");
  }

  @Test(expected = ParseException.class)
  public void testInvalidNumber() throws Exception {
    new TimeDuration("ms");
  }

  @Test(expected = ParseException.class)
  public void testNegativeDuration() throws Exception {
    new TimeDuration("-10ms");
  }
}