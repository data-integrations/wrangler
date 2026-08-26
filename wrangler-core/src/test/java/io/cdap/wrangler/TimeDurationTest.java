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

package io.cdap.wrangler;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link TimeDuration} class.
 */
public class TimeDurationTest {

  @Test
  public void testTimeDurationConstruction() {
    TimeDuration t1 = new TimeDuration("10s");
    Assert.assertEquals(10L, t1.getNanoseconds());
    
    TimeDuration t2 = new TimeDuration("1.5ms");
    Assert.assertEquals(1500000L, t2.getNanoseconds());
    
    TimeDuration t3 = new TimeDuration("2s");
    Assert.assertEquals(2 * 1000000000L, t3.getNanoseconds());
    
    TimeDuration t4 = new TimeDuration("3.5m");
    Assert.assertEquals((long) (3.5 * 60 * 1000000000L), t4.getNanoseconds());
    
    TimeDuration t5 = new TimeDuration("1h");
    Assert.assertEquals(60L * 60 * 1000000000L, t5.getNanoseconds());
    
    TimeDuration t6 = new TimeDuration("0.5d");
    Assert.assertEquals((long) (0.5 * 24 * 60 * 60 * 1000000000L), t6.getNanoseconds());
  }

  @Test
  public void testTimeDurationGetters() {
    TimeDuration t = new TimeDuration("1000ms");
    
    Assert.assertEquals(1000.0, t.getValue(), 0.001);
    Assert.assertEquals("ms", t.getUnit());
    Assert.assertEquals(1000000000L, t.getNanoseconds());
    Assert.assertEquals(1000.0, t.getMilliseconds(), 0.001);
    Assert.assertEquals(1.0, t.getSeconds(), 0.001);
    Assert.assertEquals(1.0 / 60, t.getMinutes(), 0.0001);
  }

  @Test
  public void testTimeDurationConversion() {
    TimeDuration t = new TimeDuration("2.5s");
    
    Assert.assertEquals(2.5 * 1000000000L, t.convertTo("ns"), 0.001);
    Assert.assertEquals(2.5 * 1000, t.convertTo("ms"), 0.001);
    Assert.assertEquals(2.5, t.convertTo("s"), 0.001);
    Assert.assertEquals(2.5 / 60, t.convertTo("m"), 0.00001);
    Assert.assertEquals(2.5 / (60 * 60), t.convertTo("h"), 0.00001);
    Assert.assertEquals(2.5 / (24 * 60 * 60), t.convertTo("d"), 0.00001);
  }

  @Test
  public void testEqualsAndHashCode() {
    TimeDuration t1 = new TimeDuration("1s");
    TimeDuration t2 = new TimeDuration("1000ms");
    TimeDuration t3 = new TimeDuration("2s");
    
    Assert.assertEquals(t1, t2);
    Assert.assertEquals(t1.hashCode(), t2.hashCode());
    
    Assert.assertNotEquals(t1, t3);
    Assert.assertNotEquals(t1.hashCode(), t3.hashCode());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeDuration() {
    new TimeDuration("10xs");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeDurationFormat() {
    new TimeDuration("ms");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeDurationConversion() {
    TimeDuration t = new TimeDuration("10s");
    t.convertTo("xs");
  }
}
