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
  public void testMillisecondParsing() {
    TimeDuration duration = new TimeDuration("1000ms");
    Assert.assertEquals(1000L, duration.getMilliseconds());
    Assert.assertEquals(1.0, duration.getSeconds(), 0.001);
  }

  @Test
  public void testSecondParsing() {
    TimeDuration duration = new TimeDuration("1s");
    Assert.assertEquals(1000L, duration.getMilliseconds());
    Assert.assertEquals(1.0, duration.getSeconds(), 0.001);
  }

  @Test
  public void testMinuteParsing() {
    TimeDuration duration = new TimeDuration("1m");
    Assert.assertEquals(60 * 1000L, duration.getMilliseconds());
    Assert.assertEquals(60.0, duration.getSeconds(), 0.001);
  }

  @Test
  public void testHourParsing() {
    TimeDuration duration = new TimeDuration("1h");
    Assert.assertEquals(60 * 60 * 1000L, duration.getMilliseconds());
    Assert.assertEquals(3600.0, duration.getSeconds(), 0.001);
  }

  @Test
  public void testDecimalValues() {
    TimeDuration duration = new TimeDuration("1.5s");
    Assert.assertEquals(1500L, duration.getMilliseconds());
    Assert.assertEquals(1.5, duration.getSeconds(), 0.001);
  }
}
