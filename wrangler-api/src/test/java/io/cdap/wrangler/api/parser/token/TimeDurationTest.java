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

package io.cdap.wrangler.api.parser.token;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link TimeDuration} class.
 */
public class TimeDurationTest {
  // Constants for time unit conversions
  private static final long NANOS_PER_MILLI = 1_000_000L;
  private static final long NANOS_PER_SECOND = 1_000_000_000L;
  private static final long MILLIS_PER_SECOND = 1_000L;
  private static final long SECONDS_PER_MINUTE = 60L;
  private static final long MINUTES_PER_HOUR = 60L;
  private static final long HOURS_PER_DAY = 24L;
  
  // Test values
  private static final long THOUSAND_MILLIS = 1_000L;
  private static final long HALF_SECOND_MILLIS = 500L;
  private static final long SIXTY_SECONDS = 60L;
  private static final long TWENTY_FOUR_HOURS = 24L;
  
  // Constants for precision in tests
  private static final double SMALL_DELTA = 0.000_000_1;
  private static final double STANDARD_DELTA = 0.001;
  private static final double MEDIUM_DELTA = 0.01;

  /**
   * Tests conversion between different time duration units.
   */
  @Test
  public void testTimeConversions() {
    // Test 1 second in various units
    TimeDuration oneSecondDuration = new TimeDuration("1s");
    Assert.assertEquals(NANOS_PER_SECOND, oneSecondDuration.getNanos());
    Assert.assertEquals(MILLIS_PER_SECOND, oneSecondDuration.getMillis(), STANDARD_DELTA);
    Assert.assertEquals(1, oneSecondDuration.getSeconds(), STANDARD_DELTA);
    Assert.assertEquals(1.0 / SECONDS_PER_MINUTE, oneSecondDuration.getMinutes(), SMALL_DELTA);
    
    // Test 1 minute in various units
    TimeDuration oneMinuteDuration = new TimeDuration("1m");
    Assert.assertEquals(SECONDS_PER_MINUTE * NANOS_PER_SECOND, oneMinuteDuration.getNanos());
    Assert.assertEquals(SECONDS_PER_MINUTE * MILLIS_PER_SECOND, oneMinuteDuration.getMillis(), STANDARD_DELTA);
    Assert.assertEquals(SECONDS_PER_MINUTE, oneMinuteDuration.getSeconds(), STANDARD_DELTA);
    Assert.assertEquals(1, oneMinuteDuration.getMinutes(), STANDARD_DELTA);
    
    // Use assertEquals with delta instead of assertTrue
    double expectedHours = 1.0 / MINUTES_PER_HOUR;
    Assert.assertEquals(expectedHours, oneMinuteDuration.getHours(), SMALL_DELTA);
    
    // Test 1 hour in various units
    TimeDuration oneHourDuration = new TimeDuration("1h");
    Assert.assertEquals(MINUTES_PER_HOUR * SECONDS_PER_MINUTE * NANOS_PER_SECOND, oneHourDuration.getNanos());
    Assert.assertEquals(MINUTES_PER_HOUR * SECONDS_PER_MINUTE * MILLIS_PER_SECOND, oneHourDuration.getMillis(),
            STANDARD_DELTA);
    Assert.assertEquals(MINUTES_PER_HOUR * SECONDS_PER_MINUTE, oneHourDuration.getSeconds(), STANDARD_DELTA);
    Assert.assertEquals(MINUTES_PER_HOUR, oneHourDuration.getMinutes(), STANDARD_DELTA);
    Assert.assertEquals(1, oneHourDuration.getHours(), STANDARD_DELTA);
    Assert.assertEquals(1.0 / HOURS_PER_DAY, oneHourDuration.getDays(), STANDARD_DELTA);
    
    // Test 1 day in various units
    TimeDuration oneDayDuration = new TimeDuration("1d");
    Assert.assertEquals(HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE * NANOS_PER_SECOND, 
                        oneDayDuration.getNanos());
    Assert.assertEquals(HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE * MILLIS_PER_SECOND, 
                        oneDayDuration.getMillis(), STANDARD_DELTA);
    Assert.assertEquals(HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE, 
                        oneDayDuration.getSeconds(), STANDARD_DELTA);
    Assert.assertEquals(HOURS_PER_DAY * MINUTES_PER_HOUR, oneDayDuration.getMinutes(), STANDARD_DELTA);
    Assert.assertEquals(HOURS_PER_DAY, oneDayDuration.getHours(), STANDARD_DELTA);
    Assert.assertEquals(1, oneDayDuration.getDays(), STANDARD_DELTA);
  }

  /**
   * Tests basic time duration values in different units.
   */
  @Test
  public void testBasicTimeValues() {
    // Test nanoseconds
    TimeDuration nanosecondDuration = new TimeDuration("1000ns");
    Assert.assertEquals(1000, nanosecondDuration.getNanos());
    
    // Use assertEquals with delta instead of assertTrue
    double expectedMillis = 1000.0 / NANOS_PER_MILLI;
    Assert.assertEquals(expectedMillis, nanosecondDuration.getMillis(), SMALL_DELTA);
    
    // Fix the expected value to match the actual result
    Assert.assertEquals(1.0E-6, nanosecondDuration.getSeconds(), SMALL_DELTA);
    Assert.assertEquals("ns", nanosecondDuration.getUnit());
    Assert.assertEquals(1000.0, nanosecondDuration.getNumericValue(), STANDARD_DELTA);
    
    // Test milliseconds
    TimeDuration millisecondDuration = new TimeDuration("500ms");
    Assert.assertEquals(HALF_SECOND_MILLIS * NANOS_PER_MILLI, millisecondDuration.getNanos());
    Assert.assertEquals(HALF_SECOND_MILLIS, millisecondDuration.getMillis(), STANDARD_DELTA);
    Assert.assertEquals(HALF_SECOND_MILLIS / (double) MILLIS_PER_SECOND, millisecondDuration.getSeconds(), SMALL_DELTA);
    Assert.assertEquals("ms", millisecondDuration.getUnit());
    Assert.assertEquals(HALF_SECOND_MILLIS, millisecondDuration.getNumericValue(), STANDARD_DELTA);
    
    // Test seconds with decimal point
    TimeDuration secondDuration = new TimeDuration("1.5s");
    Assert.assertEquals(1.5 * MILLIS_PER_SECOND, secondDuration.getMillis(), STANDARD_DELTA);
    Assert.assertEquals(1.5, secondDuration.getSeconds(), STANDARD_DELTA);
    Assert.assertEquals("s", secondDuration.getUnit());
    Assert.assertEquals(1.5, secondDuration.getNumericValue(), STANDARD_DELTA);
    
    // Test minutes
    TimeDuration minuteDuration = new TimeDuration("2m");
    Assert.assertEquals(2 * SECONDS_PER_MINUTE, minuteDuration.getSeconds(), STANDARD_DELTA);
    Assert.assertEquals(2, minuteDuration.getMinutes(), STANDARD_DELTA);
    Assert.assertEquals("m", minuteDuration.getUnit());
    
    // Test hours
    TimeDuration hourDuration = new TimeDuration("1h");
    Assert.assertEquals(MINUTES_PER_HOUR, hourDuration.getMinutes(), STANDARD_DELTA);
    Assert.assertEquals(1, hourDuration.getHours(), STANDARD_DELTA);
    Assert.assertEquals("h", hourDuration.getUnit());
    
    // Test days
    TimeDuration dayDuration = new TimeDuration("0.5d");
    Assert.assertEquals(HOURS_PER_DAY / 2, dayDuration.getHours(), STANDARD_DELTA);
    Assert.assertEquals(0.5, dayDuration.getDays(), STANDARD_DELTA); // Use delta here
    Assert.assertEquals("d", dayDuration.getUnit());
  }
  
  /**
   * Tests that equivalent time representations have the same value.
   */
  @Test
  public void testSpecificTimeEquivalences() {
    // Test equivalent time representations in different units
    TimeDuration thousandMillisecondsDuration = new TimeDuration("1000ms");
    TimeDuration oneSecondDuration = new TimeDuration("1s");
    Assert.assertEquals(thousandMillisecondsDuration.getNanos(), oneSecondDuration.getNanos());
    
    TimeDuration sixtySecondsDuration = new TimeDuration("60s");
    TimeDuration oneMinuteDuration = new TimeDuration("1m");
    Assert.assertEquals(sixtySecondsDuration.getNanos(), oneMinuteDuration.getNanos());
    
    TimeDuration sixtyMinutesDuration = new TimeDuration(MINUTES_PER_HOUR + "m");
    TimeDuration oneHourDuration = new TimeDuration("1h");
    Assert.assertEquals(sixtyMinutesDuration.getNanos(), oneHourDuration.getNanos());
    
    TimeDuration twentyFourHoursDuration = new TimeDuration(HOURS_PER_DAY + "h");
    TimeDuration oneDayDuration = new TimeDuration("1d");
    Assert.assertEquals(twentyFourHoursDuration.getNanos(), oneDayDuration.getNanos());
  }
  
  /**
   * Tests case insensitivity for time units.
   */
  @Test
  public void testCaseInsensitivity() {
    // Test case insensitivity for all units
    TimeDuration lowerCaseNanosecondsDuration = new TimeDuration("1000ns");
    TimeDuration upperCaseNanosecondsDuration = new TimeDuration("1000NS");
    Assert.assertEquals(lowerCaseNanosecondsDuration.getNanos(), upperCaseNanosecondsDuration.getNanos());
    
    TimeDuration lowerCaseMillisecondsDuration = new TimeDuration("500ms");
    TimeDuration upperCaseMillisecondsDuration = new TimeDuration("500MS");
    Assert.assertEquals(lowerCaseMillisecondsDuration.getNanos(), upperCaseMillisecondsDuration.getNanos());
    
    TimeDuration lowerCaseSecondsDuration = new TimeDuration("10s");
    TimeDuration upperCaseSecondsDuration = new TimeDuration("10S");
    Assert.assertEquals(lowerCaseSecondsDuration.getNanos(), upperCaseSecondsDuration.getNanos());
    
    TimeDuration lowerCaseMinutesDuration = new TimeDuration("5m");
    TimeDuration upperCaseMinutesDuration = new TimeDuration("5M");
    Assert.assertEquals(lowerCaseMinutesDuration.getNanos(), upperCaseMinutesDuration.getNanos());
    
    TimeDuration lowerCaseHoursDuration = new TimeDuration("2h");
    TimeDuration upperCaseHoursDuration = new TimeDuration("2H");
    Assert.assertEquals(lowerCaseHoursDuration.getNanos(), upperCaseHoursDuration.getNanos());
    
    TimeDuration lowerCaseDaysDuration = new TimeDuration("1d");
    TimeDuration upperCaseDaysDuration = new TimeDuration("1D");
    Assert.assertEquals(lowerCaseDaysDuration.getNanos(), upperCaseDaysDuration.getNanos());
  }
  
  /**
   * Tests handling of zero values in different time units.
   */
  @Test
  public void testZeroValues() {
    // Test zero time durations in various units
    TimeDuration zeroSecondsDuration = new TimeDuration("0s");
    Assert.assertEquals(0, zeroSecondsDuration.getNanos());
    Assert.assertEquals(0.0, zeroSecondsDuration.getMillis(), SMALL_DELTA);
    Assert.assertEquals(0.0, zeroSecondsDuration.getSeconds(), SMALL_DELTA);
    Assert.assertEquals("s", zeroSecondsDuration.getUnit());
    
    TimeDuration zeroMillisecondsDuration = new TimeDuration("0ms");
    Assert.assertEquals(0, zeroMillisecondsDuration.getNanos());
    Assert.assertEquals("ms", zeroMillisecondsDuration.getUnit());
    
    TimeDuration zeroMinutesDuration = new TimeDuration("0m");
    Assert.assertEquals(0, zeroMinutesDuration.getNanos());
    Assert.assertEquals("m", zeroMinutesDuration.getUnit());
  }
  
  /**
   * Tests whitespace handling in time duration strings.
   */
  @Test
  public void testWhitespaceHandling() {
    // Test whitespace handling between the value and unit
    TimeDuration withoutSpaceDuration = new TimeDuration("10s");
    TimeDuration withSpaceDuration = new TimeDuration("10 s");
    Assert.assertEquals(withoutSpaceDuration.getNanos(), withSpaceDuration.getNanos());
    
    TimeDuration withMultipleSpacesDuration = new TimeDuration("10   s");
    Assert.assertEquals(withoutSpaceDuration.getNanos(), withMultipleSpacesDuration.getNanos());
  }
  
  /**
   * Tests fractional time duration values.
   */
  @Test
  public void testFractionalTimeValues() {
    // Test fractional time values in different units
    TimeDuration fractionalMillisecondsDuration = new TimeDuration("0.5ms");
    Assert.assertEquals(500_000, fractionalMillisecondsDuration.getNanos());
    Assert.assertEquals(0.5, fractionalMillisecondsDuration.getMillis(), SMALL_DELTA);
    
    TimeDuration fractionalSecondsDuration = new TimeDuration("0.25s");
    Assert.assertEquals(250, fractionalSecondsDuration.getMillis(), STANDARD_DELTA);
    Assert.assertEquals(0.25, fractionalSecondsDuration.getSeconds(), SMALL_DELTA);
    
    TimeDuration fractionalMinutesDuration = new TimeDuration("0.5m");
    Assert.assertEquals(30, fractionalMinutesDuration.getSeconds(), STANDARD_DELTA);
    
    // Use assertEquals with delta instead of assertTrue
    double expectedMinutes = 0.5;
    Assert.assertEquals(expectedMinutes, fractionalMinutesDuration.getMinutes(), SMALL_DELTA);
    
    TimeDuration fractionalHoursDuration = new TimeDuration("0.25h");
    Assert.assertEquals(15, fractionalHoursDuration.getMinutes(), STANDARD_DELTA);
    Assert.assertEquals(0.25, fractionalHoursDuration.getHours(), STANDARD_DELTA);
    
    TimeDuration fractionalDaysDuration = new TimeDuration("0.125d");
    Assert.assertEquals(3, fractionalDaysDuration.getHours(), STANDARD_DELTA);
    Assert.assertEquals(0.125, fractionalDaysDuration.getDays(), STANDARD_DELTA);
  }
  
  /**
   * Tests toString method returns the original string representation.
   */
  @Test
  public void testToString() {
    // Test toString() returns original input
    String secondsDurationString = "30s";
    TimeDuration secondsDurationObject = new TimeDuration(secondsDurationString);
    Assert.assertEquals(secondsDurationString, secondsDurationObject.toString());
    
    String minutesDurationString = "2.5m";
    TimeDuration minutesDurationObject = new TimeDuration(minutesDurationString);
    Assert.assertEquals(minutesDurationString, minutesDurationObject.toString());
  }
  
  /**
   * Tests large time duration values.
   */
  @Test
  public void testLargeTimeValues() {
    // Test large time values in various units
    TimeDuration largeNanosecondsDuration = new TimeDuration("9999999999ns");
    Assert.assertEquals(9_999_999_999L, largeNanosecondsDuration.getNanos());
    
    // Use assertEquals with delta instead of assertTrue
    double expectedMillis = 9_999_999_999.0 / NANOS_PER_MILLI;
    Assert.assertEquals(expectedMillis, largeNanosecondsDuration.getMillis(), SMALL_DELTA);
    
    TimeDuration largeMillisecondsDuration = new TimeDuration("999999ms");
    Assert.assertEquals(999999 * NANOS_PER_MILLI, largeMillisecondsDuration.getNanos());
    Assert.assertEquals(999.999, largeMillisecondsDuration.getSeconds(), STANDARD_DELTA);
    
    TimeDuration largeSecondsDuration = new TimeDuration("86400s");
    Assert.assertEquals(HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE * NANOS_PER_SECOND, 
                      largeSecondsDuration.getNanos());
    Assert.assertEquals(HOURS_PER_DAY * MINUTES_PER_HOUR, largeSecondsDuration.getMinutes(), STANDARD_DELTA);
    Assert.assertEquals(HOURS_PER_DAY, largeSecondsDuration.getHours(), STANDARD_DELTA);
    Assert.assertEquals(1, largeSecondsDuration.getDays(), STANDARD_DELTA);
    
    TimeDuration largeMinutesDuration = new TimeDuration("1440m");
    Assert.assertEquals(HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE * NANOS_PER_SECOND, 
                      largeMinutesDuration.getNanos());
    Assert.assertEquals(HOURS_PER_DAY, largeMinutesDuration.getHours(), STANDARD_DELTA);
    Assert.assertEquals(1, largeMinutesDuration.getDays(), STANDARD_DELTA);
    
    TimeDuration largeHoursDuration = new TimeDuration("48h");
    Assert.assertEquals(2 * HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE * NANOS_PER_SECOND, 
                      largeHoursDuration.getNanos());
    Assert.assertEquals(2, largeHoursDuration.getDays(), STANDARD_DELTA);
    
    TimeDuration largeDaysDuration = new TimeDuration("365d");
    Assert.assertEquals(365 * HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE * NANOS_PER_SECOND, 
                      largeDaysDuration.getNanos());
    Assert.assertEquals(365, largeDaysDuration.getDays(), STANDARD_DELTA);
  }
  
  /**
   * Tests extreme values including very small and very large time durations.
   */
  @Test
  public void testExtremeValues() {
    // Test extremely small time values
    TimeDuration verySmallSecondsDuration = new TimeDuration("0.000001s");
    Assert.assertEquals(1000, verySmallSecondsDuration.getNanos()); // 1 microsecond = 1000 nanoseconds
    
    // Test extremely large values 
    TimeDuration veryLargeDaysDuration = new TimeDuration("3650d"); // 10 years
    Assert.assertTrue(veryLargeDaysDuration.getNanos() > 0); // Should be positive
    
    // Edge case exactly at time boundary
    TimeDuration exactMinuteBoundaryDuration = new TimeDuration("60s");
    Assert.assertEquals(1.0, exactMinuteBoundaryDuration.getMinutes(), SMALL_DELTA);
  }
  
  /**
   * Tests preservation of precision in time calculations.
   */
  @Test
  public void testPrecisionPreservation() {
    // Test that precision is preserved in calculations
    TimeDuration preciseMillisecondsDuration = new TimeDuration("0.123456ms");
    Assert.assertEquals(123456, preciseMillisecondsDuration.getNanos());
    
    TimeDuration preciseSecondsDuration = new TimeDuration("0.123456s");
    Assert.assertEquals(123456000, preciseSecondsDuration.getNanos());
  }
  
  /**
   * Tests that TimeDuration implements the Token interface correctly.
   */
  @Test
  public void testTokenInterface() {
    // Test Token interface methods
    TimeDuration secondsDuration = new TimeDuration("10s");
    Assert.assertEquals("10s", secondsDuration.value());
    Assert.assertEquals(TokenType.TIME_DURATION, secondsDuration.type());
    
    // Test JSON representation
    JsonElement jsonElement = secondsDuration.toJson();
    Assert.assertTrue(jsonElement instanceof JsonObject);
    JsonObject jsonObject = (JsonObject) jsonElement;
    
    Assert.assertEquals("TIME_DURATION", jsonObject.get("type").getAsString());
    Assert.assertEquals("10s", jsonObject.get("value").getAsString());
    Assert.assertEquals(10 * NANOS_PER_SECOND, jsonObject.get("nanos").getAsLong());
  }
  
  /**
   * Tests that invalid format throws appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    new TimeDuration("10x"); // Invalid unit should throw exception
  }
  
  /**
   * Tests that non-numeric value throws appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNumber() {
    new TimeDuration("ABCs"); // Not a number should throw exception
  }
  
  /**
   * Tests that missing unit throws appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testMissingUnit() {
    new TimeDuration("10"); // Missing unit should throw exception
  }
  
  /**
   * Tests that incorrect unit format throws appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIncorrectUnitFormat() {
    new TimeDuration("10sec"); // Incorrect unit format should throw exception
  }
  
  /**
   * Tests that negative values throw appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNegativeValue() {
    new TimeDuration("-10s"); // Negative time values should throw exception
  }
  
  /**
   * Tests the new getUnitName method.
   */
  @Test
  public void testGetUnitName() {
    TimeDuration nanosecondsDuration = new TimeDuration("500ns");
    Assert.assertEquals("nanoseconds", nanosecondsDuration.getUnitName());
    
    TimeDuration millisecondsDuration = new TimeDuration("100ms");
    Assert.assertEquals("milliseconds", millisecondsDuration.getUnitName());
    
    TimeDuration secondsDuration = new TimeDuration("10s");
    Assert.assertEquals("seconds", secondsDuration.getUnitName());
    
    TimeDuration minutesDuration = new TimeDuration("5m");
    Assert.assertEquals("minutes", minutesDuration.getUnitName());
    
    TimeDuration hoursDuration = new TimeDuration("2h");
    Assert.assertEquals("hours", hoursDuration.getUnitName());
    
    TimeDuration daysDuration = new TimeDuration("1d");
    Assert.assertEquals("days", daysDuration.getUnitName());
  }
  
  /**
   * Tests the new convertToUnit method.
   */
  @Test
  public void testConvertToUnit() {
    // Test converting from seconds to other units
    TimeDuration tenSecondsDuration = new TimeDuration("10s");
    
    TimeDuration toNanos = tenSecondsDuration.convertToUnit("ns");
    Assert.assertEquals("ns", toNanos.getUnit());
    Assert.assertEquals(10 * NANOS_PER_SECOND, toNanos.getNanos());
    
    TimeDuration toMillis = tenSecondsDuration.convertToUnit("ms");
    Assert.assertEquals("ms", toMillis.getUnit());
    Assert.assertEquals(10 * MILLIS_PER_SECOND, toMillis.getNumericValue(), STANDARD_DELTA);
    
    TimeDuration toMinutes = tenSecondsDuration.convertToUnit("m");
    Assert.assertEquals("m", toMinutes.getUnit());
    Assert.assertEquals(10.0 / SECONDS_PER_MINUTE, toMinutes.getNumericValue(), SMALL_DELTA);
    
    // Test case-insensitivity in conversion
    TimeDuration toHoursUppercase = tenSecondsDuration.convertToUnit("H");
    Assert.assertEquals("h", toHoursUppercase.getUnit());
    Assert.assertEquals(10.0 / (SECONDS_PER_MINUTE * MINUTES_PER_HOUR), toHoursUppercase.getNumericValue(),
            SMALL_DELTA);
  }
  
  /**
   * Tests that invalid target unit in convertToUnit throws appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConversionUnit() {
    TimeDuration duration = new TimeDuration("10s");
    duration.convertToUnit("invalid"); // Should throw IllegalArgumentException
  }
  
  /**
   * Tests the new isEquivalentTo method.
   */
  @Test
  public void testIsEquivalentTo() {
    TimeDuration oneMinute = new TimeDuration("1m");
    TimeDuration sixtySeconds = new TimeDuration("60s");
    TimeDuration oneHundredTwentySeconds = new TimeDuration("120s");
    
    Assert.assertTrue("One minute should be equivalent to 60 seconds", 
                      oneMinute.isEquivalentTo(sixtySeconds));
    Assert.assertTrue("60 seconds should be equivalent to one minute", 
                      sixtySeconds.isEquivalentTo(oneMinute));
    Assert.assertFalse("One minute should not be equivalent to 120 seconds", 
                       oneMinute.isEquivalentTo(oneHundredTwentySeconds));
    Assert.assertFalse("Null comparison should return false", 
                       oneMinute.isEquivalentTo(null));
  }
  
  /**
   * Tests equals and hashCode implementations.
   */
  @Test
  public void testEqualsAndHashCode() {
    TimeDuration duration1 = new TimeDuration("60s");
    TimeDuration duration2 = new TimeDuration("1m");
    TimeDuration duration3 = new TimeDuration("60s");
    TimeDuration differentDuration = new TimeDuration("30s");
    
    // Test reflexivity
    Assert.assertEquals(duration1, duration1);
    
    // Test symmetry
    Assert.assertEquals(duration1, duration3);
    Assert.assertEquals(duration3, duration1);
    
    // Test equivalence relationship
    Assert.assertEquals(duration1, duration2);
    Assert.assertEquals(duration2, duration3);
    
    // Test inequality
    Assert.assertNotEquals(duration1, differentDuration);
    Assert.assertNotEquals(differentDuration, duration1);
    
    // Test null and different type handling
    Assert.assertNotEquals(duration1, null);
    Assert.assertNotEquals(duration1, "String");
    
    // Test hashCode consistency with equals
    Assert.assertEquals(duration1.hashCode(), duration2.hashCode());
    Assert.assertEquals(duration2.hashCode(), duration3.hashCode());
    Assert.assertNotEquals(duration1.hashCode(), differentDuration.hashCode());
    
    // Test in collections
    Set<TimeDuration> durationSet = new HashSet<>();
    durationSet.add(duration1);
    durationSet.add(duration2); // Should not increase size since equivalent to duration1
    durationSet.add(duration3); // Should not increase size since equals duration1
    Assert.assertEquals("Set should contain only one unique duration", 1, durationSet.size());
    
    durationSet.add(differentDuration);
    Assert.assertEquals("Set should now contain two unique durations", 2, durationSet.size());
  }
  
  /**
   * Tests the enhanced JSON representation.
   */
  @Test
  public void testEnhancedJsonRepresentation() {
    TimeDuration duration = new TimeDuration("10.5m");
    JsonObject json = (JsonObject) duration.toJson();
    
    Assert.assertEquals("TIME_DURATION", json.get("type").getAsString());
    Assert.assertEquals("10.5m", json.get("value").getAsString());
    Assert.assertEquals(10.5 * SECONDS_PER_MINUTE * NANOS_PER_SECOND, json.get("nanos").getAsLong());
    Assert.assertEquals("m", json.get("unit").getAsString());
    Assert.assertEquals(10.5, json.get("numericValue").getAsDouble(), SMALL_DELTA);
  }
}
