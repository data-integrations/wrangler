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

import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

  @Test
  public void testParseTimeDuration() {
    // Test basic parsing
    TimeDuration duration1 = new TimeDuration("150ms");
    Assert.assertEquals(150 * 1000000L, duration1.value().longValue());
    Assert.assertEquals(150.0, duration1.getValue("ms"), 0.001);

    TimeDuration duration2 = new TimeDuration("2.1s");
    Assert.assertEquals((long)(2.1 * 1000000000), duration2.value().longValue());
    Assert.assertEquals(2.1, duration2.getValue("s"), 0.001);

    // Test different unit formats
    TimeDuration duration3 = new TimeDuration("5m");
    Assert.assertEquals(5L * 60 * 1000000000, duration3.value().longValue());
    Assert.assertEquals(5.0, duration3.getValue("m"), 0.001);

    // Test nanoseconds
    TimeDuration duration4 = new TimeDuration("1000ns");
    Assert.assertEquals(1000L, duration4.value().longValue());
    Assert.assertEquals(1000.0, duration4.getValue("ns"), 0.001);

    // Test microseconds
    TimeDuration duration5 = new TimeDuration("1000us");
    Assert.assertEquals(1000L * 1000, duration5.value().longValue());
    Assert.assertEquals(1000.0, duration5.getValue("us"), 0.001);

    // Test large values
    TimeDuration duration6 = new TimeDuration("1d");
    Assert.assertEquals(24L * 60 * 60 * 1000000000, duration6.value().longValue());
    Assert.assertEquals(1.0, duration6.getValue("d"), 0.001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    new TimeDuration("invalid");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    new TimeDuration("10XX");
  }

  @Test
  public void testEmptyValue() {
    TimeDuration duration = new TimeDuration("");
    Assert.assertEquals(0L, duration.value().longValue());
  }

  @Test
  public void testUnitConversion() {
    TimeDuration duration = new TimeDuration("1h");
    
    // Test conversion to different units
    Assert.assertEquals(60.0, duration.getValue("m"), 0.001);
    Assert.assertEquals(3600.0, duration.getValue("s"), 0.001);
    Assert.assertEquals(3600000.0, duration.getValue("ms"), 0.001);
    Assert.assertEquals(3600000000.0, duration.getValue("us"), 0.001);
    Assert.assertEquals(3600000000000.0, duration.getValue("ns"), 0.001);
    Assert.assertEquals(0.0416666667, duration.getValue("d"), 0.001);
  }
} 