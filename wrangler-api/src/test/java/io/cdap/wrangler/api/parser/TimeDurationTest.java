/*
 * Copyright © 2025 CDAP
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

import io.cdap.wrangler.api.DirectiveParseException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link TimeDuration}
 */
public class TimeDurationTest {

    private static final long NANO = 1L;
    private static final long MICRO = 1000L * NANO;
    private static final long MILLI = 1000L * MICRO;
    private static final long SECOND = 1000L * MILLI;
    private static final long MINUTE = 60L * SECOND;
    private static final long HOUR = 60L * MINUTE;
    private static final long DAY = 24L * HOUR;

    @Test
    public void testValidDurations() throws DirectiveParseException {
        Assert.assertEquals(10L * NANO, new TimeDuration("10ns").getNanoseconds());
        Assert.assertEquals(10L * NANO, new TimeDuration("10NS").getNanoseconds()); // Case insensitive
        Assert.assertEquals(50L * MICRO, new TimeDuration("50us").getNanoseconds());
        Assert.assertEquals(50L * MICRO, new TimeDuration("50µs").getNanoseconds()); // Micro symbol
        Assert.assertEquals(150L * MILLI, new TimeDuration("150ms").getNanoseconds());
        Assert.assertEquals(5L * SECOND, new TimeDuration("5s").getNanoseconds());
        Assert.assertEquals(2L * MINUTE, new TimeDuration("2m").getNanoseconds());
        Assert.assertEquals(3L * HOUR, new TimeDuration("3h").getNanoseconds());
        Assert.assertEquals(1L * DAY, new TimeDuration("1d").getNanoseconds());
        Assert.assertEquals(0L, new TimeDuration("0s").getNanoseconds());

        // Test default unit (seconds)
        Assert.assertEquals(10L * SECOND, new TimeDuration("10").getNanoseconds());
        Assert.assertEquals(10L * SECOND, new TimeDuration(" 10 ").getNanoseconds()); // Spaces

        // Test doubles
        Assert.assertEquals((long) (2.5 * SECOND), new TimeDuration("2.5s").getNanoseconds());
        Assert.assertEquals((long) (1.5 * MINUTE), new TimeDuration("1.5m").getNanoseconds());
        Assert.assertEquals((long) (0.5 * HOUR), new TimeDuration("0.5h").getNanoseconds());
        Assert.assertEquals((long) (0.1 * SECOND), new TimeDuration("0.1").getNanoseconds()); // Default seconds
    }

    @Test
    public void testFractionalRounding() throws DirectiveParseException {
        // 1.9ms = 1,900,000 ns -> 1,900,000
        Assert.assertEquals(1_900_000L, new TimeDuration("1.9ms").getNanoseconds());
        // 0.1us = 100 ns -> 100
        Assert.assertEquals(100L, new TimeDuration("0.1us").getNanoseconds());
        // 5.1ns -> 5
        Assert.assertEquals(5L, new TimeDuration("5.1ns").getNanoseconds());
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidFormatString() throws DirectiveParseException {
        new TimeDuration("abc");
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidFormatUnitOnly() throws DirectiveParseException {
        new TimeDuration("ms");
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidFormatNegative() throws DirectiveParseException {
        new TimeDuration("-10s");
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidFormatUnknownUnit() throws DirectiveParseException {
        new TimeDuration("10years"); // years is not a valid unit
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidFormatMultipleUnits() throws DirectiveParseException {
        new TimeDuration("10ms s");
    }

    @Test(expected = DirectiveParseException.class)
    public void testOverflow() throws DirectiveParseException {
        // A value slightly larger than Long.MAX_VALUE / NANOS_PER_DAY for days
        double nearOverflowDays = ((double) Long.MAX_VALUE / DAY) + 1;
        new TimeDuration(String.format("%.0fd", nearOverflowDays));
    }
}
