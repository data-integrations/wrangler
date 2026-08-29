/*
 * Copyright © 2024 Cask Data, Inc.
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

import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

  @Test
  public void testValidTimeDurations() {
    // Test nanoseconds
    TimeDuration ns = new TimeDuration("1000ns");
    Assert.assertEquals(1000L, ns.getNanoseconds());
    Assert.assertEquals(1.0, ns.getMicroseconds(), 0.001);

    // Test microseconds
    TimeDuration us = new TimeDuration("1.5us");
    Assert.assertEquals(1500L, us.getNanoseconds());
    Assert.assertEquals(1.5, us.getMicroseconds(), 0.001);

    // Test milliseconds
    TimeDuration ms = new TimeDuration("2.5ms");
    Assert.assertEquals(2500000L, ms.getNanoseconds());
    Assert.assertEquals(2500.0, ms.getMicroseconds(), 0.001);
    Assert.assertEquals(2.5, ms.getMilliseconds(), 0.001);

    // Test seconds
    TimeDuration s = new TimeDuration("1.5s");
    Assert.assertEquals(1500000000L, s.getNanoseconds());
    Assert.assertEquals(1500000.0, s.getMicroseconds(), 0.001);
    Assert.assertEquals(1500.0, s.getMilliseconds(), 0.001);
    Assert.assertEquals(1.5, s.getSeconds(), 0.001);

    // Test minutes
    TimeDuration m = new TimeDuration("2m");
    Assert.assertEquals(120000000000L, m.getNanoseconds());
    Assert.assertEquals(120.0, m.getSeconds(), 0.001);
    Assert.assertEquals(2.0, m.getMinutes(), 0.001);

    // Test hours
    TimeDuration h = new TimeDuration("1.5h");
    Assert.assertEquals(5400000000000L, h.getNanoseconds());
    Assert.assertEquals(90.0, h.getMinutes(), 0.001);
    Assert.assertEquals(1.5, h.getHours(), 0.001);

    // Test days
    TimeDuration d = new TimeDuration("2d");
    Assert.assertEquals(172800000000000L, d.getNanoseconds());
    Assert.assertEquals(48.0, d.getHours(), 0.001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    new TimeDuration("invalid");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    new TimeDuration("1y");
  }

  @Test
  public void testToString() {
    TimeDuration duration = new TimeDuration("1.5s");
    Assert.assertEquals("1.5s", duration.toString());
  }

  @Test
  public void testToJson() {
    TimeDuration duration = new TimeDuration("1.5s");
    Assert.assertEquals("TIME_DURATION", duration.toJson().getAsJsonObject().get("type").getAsString());
    Assert.assertEquals("1.5s", duration.toJson().getAsJsonObject().get("value").getAsString());
    Assert.assertEquals(1500000000L, duration.toJson().getAsJsonObject().get("nanos").getAsLong());
  }
} 