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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

  @Test
  public void testNanosecondParsing() {
    TimeDuration duration = new TimeDuration("100ns");
    Assert.assertEquals(100, duration.getNanoseconds());
    Assert.assertEquals(0.0001, duration.getMilliseconds(), 0.00001);
  }

  @Test
  public void testMillisecondParsing() {
    TimeDuration duration = new TimeDuration("10ms");
    Assert.assertEquals(10_000_000, duration.getNanoseconds());
    Assert.assertEquals(10, duration.getMilliseconds(), 0.00001);
    Assert.assertEquals(0.01, duration.getSeconds(), 0.00001);
  }

  @Test
  public void testSecondParsing() {
    TimeDuration duration = new TimeDuration("1.5s");
    Assert.assertEquals(1500_000_000, duration.getNanoseconds());
    Assert.assertEquals(1.5, duration.getSeconds(), 0.00001);
  }

  @Test
  public void testMinuteParsing() {
    TimeDuration duration = new TimeDuration("2min");
    Assert.assertEquals(120_000_000_000L, duration.getNanoseconds());
    Assert.assertEquals(2, duration.getMinutes(), 0.00001);
  }

  @Test
  public void testHourParsing() {
    TimeDuration duration = new TimeDuration("0.5h");
    Assert.assertEquals(1800_000_000_000L, duration.getNanoseconds());
    Assert.assertEquals(0.5, duration.getHours(), 0.00001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    new TimeDuration("10d");
  }

  @Test(expected = NumberFormatException.class)
  public void testInvalidNumber() {
    new TimeDuration("XYZ");
  }
}
