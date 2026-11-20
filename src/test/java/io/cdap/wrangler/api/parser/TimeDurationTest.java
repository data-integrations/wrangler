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

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link TimeDuration} class.
 */
public class TimeDurationTest {

  @Test
  public void testParsing() {
    // Test various valid time duration strings
    TimeDuration td1 = new TimeDuration("10ms");
    Assert.assertEquals(10_000_000, td1.getNanoseconds());
    Assert.assertEquals(TimeUnit.MILLISECONDS, td1.getUnit());
    Assert.assertEquals(10, td1.getValue(), 0.001);

    TimeDuration td2 = new TimeDuration("5s");
    Assert.assertEquals(5_000, td2.getMilliseconds());
    Assert.assertEquals(TimeUnit.SECONDS, td2.getUnit());
    Assert.assertEquals(5, td2.getValue(), 0.001);

    TimeDuration td3 = new TimeDuration("2.5m");
    Assert.assertEquals(150, td3.getSeconds());
    Assert.assertEquals(TimeUnit.MINUTES, td3.getUnit());
    Assert.assertEquals(2.5, td3.getValue(), 0.001);

    TimeDuration td4 = new TimeDuration("1.75h");
    Assert.assertEquals(105, td4.getMinutes());
    Assert.assertEquals(TimeUnit.HOURS, td4.getUnit());
    Assert.assertEquals(1.75, td4.getValue(), 0.001);

    // Case insensitivity
    TimeDuration td5 = new TimeDuration("3S");
    Assert.assertEquals(3_000, td5.getMilliseconds());
    Assert.assertEquals(TimeUnit.SECONDS, td5.getUnit());

    // Allowed whitespace
    TimeDuration td6 = new TimeDuration("4 ms");
    Assert.assertEquals(4, td6.getMilliseconds());
    Assert.assertEquals(TimeUnit.MILLISECONDS, td6.getUnit());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    // Missing unit
    new TimeDuration("10");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    // Invalid unit
    new TimeDuration("10x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonNumericValue() {
    // Non-numeric value
    new TimeDuration("abc ms");
  }

  @Test
  public void testConversion() {
    TimeDuration td = new TimeDuration("1000ms");
    
    // Test basic conversion methods
    Assert.assertEquals(1_000_000_000, td.getNanoseconds());
    Assert.assertEquals(1000, td.getMilliseconds());
    Assert.assertEquals(1, td.getSeconds());
    
    // Test convertTo method for different units
    Assert.assertEquals(1, td.convertTo(TimeUnit.SECONDS));
    Assert.assertEquals(1000, td.convertTo(TimeUnit.MILLISECONDS));
    Assert.assertEquals(1_000_000_000, td.convertTo(TimeUnit.NANOSECONDS));
    
    // Test convertToDouble method
    Assert.assertEquals(1.0, td.convertToDouble(TimeUnit.SECONDS), 0.001);
    Assert.assertEquals(0.01666, td.convertToDouble(TimeUnit.MINUTES), 0.0001);
    
    // Test converting to larger units (preserving fractional parts)
    Assert.assertEquals(0.01666, td.convertToDouble(TimeUnit.MINUTES), 0.0001);
    Assert.assertEquals(0.000277, td.convertToDouble(TimeUnit.HOURS), 0.00001);
    
    // Test to() method
    TimeDuration seconds = td.to(TimeUnit.SECONDS);
    Assert.assertEquals(TimeUnit.SECONDS, seconds.getUnit());
    Assert.assertEquals(1, seconds.getValue(), 0.001);
    Assert.assertEquals(1000, seconds.getMilliseconds());
    
    // Test conversion to larger unit
    TimeDuration minutes = new TimeDuration("120s").to(TimeUnit.MINUTES);
    Assert.assertEquals(TimeUnit.MINUTES, minutes.getUnit());
    Assert.assertEquals(2, minutes.getValue(), 0.001);
  }

  @Test
  public void testStaticMethods() {
    // Test fromMillis()
    TimeDuration td = TimeDuration.fromMillis(1000);
    Assert.assertEquals(1000, td.getMillis());
    Assert.assertEquals(TimeDuration.TimeUnit.MILLISECOND, td.getUnit());
    
    // Test parse() with null/empty
    Assert.assertNull(TimeDuration.parse(null));
    Assert.assertNull(TimeDuration.parse(""));
    Assert.assertNull(TimeDuration.parse("  "));
    
    // Test parse() with valid value
    TimeDuration parsed = TimeDuration.parse("5s");
    Assert.assertEquals(5 * 1000, parsed.getMillis());
  }

  @Test
  public void testFormat() {
    TimeDuration td = new TimeDuration(2.5, TimeDuration.TimeUnit.SECOND);
    
    // Test format with different precisions
    Assert.assertEquals("2.5s", td.format(1));
    Assert.assertEquals("2.50s", td.format(2));
    Assert.assertEquals("2.500s", td.format(3));
  }

  @Test
  public void testTokenInterface() {
    TimeDuration td = new TimeDuration("1.5h");
    
    // Test Token interface methods
    Assert.assertEquals(TokenType.TIME_DURATION, td.type());
    Assert.assertEquals(td, td.value());
    Assert.assertEquals("1.5h", td.toString());
    
    // Test toJson
    JsonObject json = td.toJson().getAsJsonObject();
    Assert.assertEquals("TIME_DURATION", json.get("type").getAsString());
    Assert.assertEquals("1.5h", json.get("value").getAsString());
    Assert.assertEquals("h", json.get("unit").getAsString());
    Assert.assertEquals((long) (1.5 * 60 * 60 * 1000), json.get("millis").getAsLong());
  }
} 
