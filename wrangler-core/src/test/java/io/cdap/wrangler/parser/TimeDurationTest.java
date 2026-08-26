/*
 * Copyright © 2017-2022 Cask Data, Inc.
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

import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link TimeDuration} class.
 */
public class TimeDurationTest {

  @Test
  public void testTimeDurationParsing() {
    // Test parsing various time duration formats
    TimeDuration t1 = new TimeDuration("10ns");
    Assert.assertEquals(10L, t1.getNanoseconds());
    Assert.assertEquals(TimeUnit.NANOSECONDS, t1.getUnit());
    Assert.assertEquals(10.0, t1.getValue(), 0.001);

    TimeDuration t2 = new TimeDuration("1.5ms");
    Assert.assertEquals(1_500_000L, t2.getNanoseconds());
    Assert.assertEquals(TimeUnit.MILLISECONDS, t2.getUnit());
    Assert.assertEquals(1.5, t2.getValue(), 0.001);
    
    TimeDuration t3 = new TimeDuration("2s");
    Assert.assertEquals(2_000_000_000L, t3.getNanoseconds());
    Assert.assertEquals(TimeUnit.SECONDS, t3.getUnit());
    
    TimeDuration t4 = new TimeDuration("3.5m");
    Assert.assertEquals(3.5 * 60 * 1_000_000_000L, t4.getNanoseconds(), 1_000_000);
    Assert.assertEquals(TimeUnit.MINUTES, t4.getUnit());
    
    TimeDuration t5 = new TimeDuration("1h");
    Assert.assertEquals(60 * 60 * 1_000_000_000L, t5.getNanoseconds());
    Assert.assertEquals(TimeUnit.HOURS, t5.getUnit());
    
    TimeDuration t6 = new TimeDuration("2d");
    Assert.assertEquals(2 * 24 * 60 * 60 * 1_000_000_000L, t6.getNanoseconds());
    Assert.assertEquals(TimeUnit.DAYS, t6.getUnit());
  }

  @Test
  public void testCaseInsensitivity() {
    // Test case insensitivity in unit parsing
    TimeDuration t1 = new TimeDuration("10NS");
    Assert.assertEquals(10L, t1.getNanoseconds());
    Assert.assertEquals(TimeUnit.NANOSECONDS, t1.getUnit());

    TimeDuration t2 = new TimeDuration("1.5MS");
    Assert.assertEquals(1_500_000L, t2.getNanoseconds());
    Assert.assertEquals(TimeUnit.MILLISECONDS, t2.getUnit());

    TimeDuration t3 = new TimeDuration("2S");
    Assert.assertEquals(2_000_000_000L, t3.getNanoseconds());
    Assert.assertEquals(TimeUnit.SECONDS, t3.getUnit());
  }

  @Test
  public void testConversions() {
    // Test conversion between units
    TimeDuration original = new TimeDuration("5s");
    
    // Convert to milliseconds
    Assert.assertEquals(5000, original.convertTo(TimeUnit.MILLISECONDS));
    
    // Convert to minutes
    Assert.assertEquals(5.0 / 60, original.convertToDouble(TimeUnit.MINUTES), 0.001);
    
    // Convert to same unit (should be unchanged)
    Assert.assertEquals(5.0, original.convertToDouble(TimeUnit.SECONDS), 0.001);
    
    // Test to() method
    TimeDuration inMs = original.to(TimeUnit.MILLISECONDS);
    Assert.assertEquals(TimeUnit.MILLISECONDS, inMs.getUnit());
    Assert.assertEquals(5000, inMs.getValue(), 0.001);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    // Test invalid format (missing unit)
    new TimeDuration("100");
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    // Test invalid unit
    new TimeDuration("100x");
  }
  
  @Test
  public void testFromNanos() {
    // Test creating TimeDuration from nanoseconds
    TimeDuration ts = TimeDuration.fromNanos(1_000_000_000);
    Assert.assertEquals(TimeUnit.NANOSECONDS, ts.getUnit());
    Assert.assertEquals(1_000_000_000, ts.getValue(), 0.001);
    
    // Convert to seconds for a more readable form
    TimeDuration inSeconds = ts.to(TimeUnit.SECONDS);
    Assert.assertEquals(TimeUnit.SECONDS, inSeconds.getUnit());
    Assert.assertEquals(1.0, inSeconds.getValue(), 0.001);
  }
  
  @Test
  public void testFromMillis() {
    // Test creating TimeDuration from milliseconds
    TimeDuration ts = TimeDuration.fromMillis(1000);
    Assert.assertEquals(TimeUnit.MILLISECONDS, ts.getUnit());
    Assert.assertEquals(1000, ts.getValue(), 0.001);
    Assert.assertEquals(1_000_000_000L, ts.getNanoseconds());
    
    // Convert to seconds for a more readable form
    TimeDuration inSeconds = ts.to(TimeUnit.SECONDS);
    Assert.assertEquals(TimeUnit.SECONDS, inSeconds.getUnit());
    Assert.assertEquals(1.0, inSeconds.getValue(), 0.001);
  }
  
  @Test
  public void testParseNullable() {
    // Test null handling
    Assert.assertNull(TimeDuration.parse(null));
    Assert.assertNull(TimeDuration.parse(""));
    Assert.assertNull(TimeDuration.parse("  "));
    
    // Test successful parsing
    TimeDuration ts = TimeDuration.parse("10ms");
    Assert.assertNotNull(ts);
    Assert.assertEquals(10_000_000L, ts.getNanoseconds());
  }
  
  @Test
  public void testFormatting() {
    // Test formatting
    TimeDuration ts = new TimeDuration("10.5s");
    Assert.assertEquals("10.5s", ts.format(1));
    Assert.assertEquals("10.50s", ts.format(2));
  }
  
  @Test
  public void testGetMillisecondsAndSeconds() {
    // Test convenience methods
    TimeDuration ts1 = new TimeDuration("1500ms");
    Assert.assertEquals(1500, ts1.getMilliseconds());
    Assert.assertEquals(1, ts1.getSeconds());
    
    TimeDuration ts2 = new TimeDuration("2.5s");
    Assert.assertEquals(2500, ts2.getMilliseconds());
    Assert.assertEquals(2, ts2.getSeconds());
  }
} 
