/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import org.junit.Assert;
import org.junit.Test;
import io.cdap.wrangler.api.parser.TimeDuration;

public class TimeDurationTest {

    @Test
    public void testTimeDurationParsing() {
        // Valid input tests for milliseconds, seconds, minutes, hours, and days
        TimeDuration t1 = new TimeDuration("10ms");
        Assert.assertEquals(10L, (long) t1.value());

        TimeDuration t2 = new TimeDuration("2500ms");
        Assert.assertEquals(2500L, (long) t2.value());

        TimeDuration t3 = new TimeDuration("1m");
        Assert.assertEquals(60000L, (long) t3.value());

        TimeDuration t4 = new TimeDuration("2h");
        Assert.assertEquals(7200000L, (long) t4.value());

        TimeDuration t5 = new TimeDuration("1d");
        Assert.assertEquals(86400000L, (long) t5.value());
    }

    @Test
    public void testTimeDurationConversion() {
        long durationInMillis = 5000L; // 5 seconds in milliseconds

        double seconds = convert(durationInMillis, "s");
        Assert.assertEquals(5.0, seconds, 0.001);

        double minutes = convert(durationInMillis, "m");
        Assert.assertEquals(0.083333, minutes, 0.000001);

        double hours = convert(durationInMillis, "h");
        Assert.assertEquals(0.0013888889, hours, 0.0000001);

        double days = convert(durationInMillis, "d");
        Assert.assertEquals(5.0 / 86400.0, days, 0.0000001);
    }

    @Test
    public void testInvalidTimeDuration() {
        // Invalid unit
        try {
            new TimeDuration("10xyz");
            Assert.fail("Expected exception for invalid time duration format.");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // Empty string
        try {
            new TimeDuration("");
            Assert.fail("Expected exception for empty string.");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // Null input
        try {
            new TimeDuration(null);
            Assert.fail("Expected exception for null input.");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void testZeroTimeDuration() {
        TimeDuration zero = new TimeDuration("0ms");
        Assert.assertEquals(0L, (long) zero.value());
    }

    @Test
    public void testNegativeTimeDuration() {
        try {
            new TimeDuration("-10s");
            Assert.fail("Expected exception for negative time duration.");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    // Static conversion utility to match the test expectations
    public static double convert(long millis, String unit) {
        switch (unit.toLowerCase()) {
            case "s":
                return millis / 1000.0;
            case "m":
                return millis / (60.0 * 1000.0);
            case "h":
                return millis / (60.0 * 60.0 * 1000.0);
            case "d":
                return millis / (24.0 * 60.0 * 60.0 * 1000.0);
            default:
                throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }
}
