/*
 * Copyright © 2023 Cask Data, Inc.
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

import org.junit.Assert;
import org.junit.Test;

import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;

/**
 * Tests for {@link TimeDuration} token class.
 */
public class TimeDurationTest {

  @Test
  public void testTimeDurationTokenType() {
    TimeDuration timeDuration = new TimeDuration("100ms");
    Assert.assertEquals(TokenType.TIME_DURATION, timeDuration.type());
  }

  @Test
  public void testTimeDurationParsing() {
    TimeDuration ns = new TimeDuration("1ns");
    TimeDuration us = new TimeDuration("1us");
    TimeDuration ms = new TimeDuration("1ms");
    TimeDuration s = new TimeDuration("1s");
    TimeDuration m = new TimeDuration("1m");
    TimeDuration h = new TimeDuration("1h");
    TimeDuration d = new TimeDuration("1d");

    Assert.assertEquals(1L, ns.getNanoseconds());
    Assert.assertEquals(1000L, us.getNanoseconds());
    Assert.assertEquals(1000000L, ms.getNanoseconds());
    Assert.assertEquals(1000000000L, s.getNanoseconds());
    Assert.assertEquals(60L * 1000000000L, m.getNanoseconds());
    Assert.assertEquals(60L * 60L * 1000000000L, h.getNanoseconds());
    Assert.assertEquals(24L * 60L * 60L * 1000000000L, d.getNanoseconds());
  }

  @Test
  public void testTimeDurationDecimalParsing() {
    TimeDuration ms = new TimeDuration("1.5ms");
    Assert.assertEquals((long) (1.5 * 1000000L), ms.getNanoseconds());
  }

  @Test
  public void testTimeDurationConversions() {
    TimeDuration ms = new TimeDuration("1ms");
    Assert.assertEquals(1.0, ms.getMilliseconds(), 0.0001);
    Assert.assertEquals(1000.0, ms.getMicroseconds(), 0.0001);
    Assert.assertEquals(1000000.0, ms.getNanoseconds(), 0.0001);
    Assert.assertEquals(0.001, ms.getSeconds(), 0.0001);
  }

  @Test
  public void testValueMethod() {
    TimeDuration timeDuration = new TimeDuration("100ms");
    Assert.assertEquals("100ms", timeDuration.value());
  }

  @Test
  public void testToStringMethod() {
    TimeDuration timeDuration = new TimeDuration("100ms");
    Assert.assertEquals("100ms", timeDuration.toString());
  }

  @Test
  public void testCaseInsensitivity() {
    TimeDuration ms1 = new TimeDuration("1ms");
    TimeDuration ms2 = new TimeDuration("1MS");
    Assert.assertEquals(ms1.getNanoseconds(), ms2.getNanoseconds());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeDurationFormat() {
    new TimeDuration("invalid");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeDurationUnit() {
    new TimeDuration("10xs");
  }
}