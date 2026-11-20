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

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link TimeDuration} token implementation.
 */
public class TimeDurationTest {

    @Test
    public void testValidTimeDurationParsing() {
        // Test seconds
        TimeDuration duration = new TimeDuration("10s");
        Assert.assertEquals(10 * 1000L, duration.getMilliseconds());
        Assert.assertEquals(10.0, duration.getSeconds(), 0.0001);
        Assert.assertEquals("10s", duration.value());

        // Test alternate seconds format
        duration = new TimeDuration("10S");
        Assert.assertEquals(10 * 1000L, duration.getMilliseconds());

        // Test minutes
        duration = new TimeDuration("5m");
        Assert.assertEquals(5 * 60 * 1000L, duration.getMilliseconds());
        Assert.assertEquals(5.0, duration.getMinutes(), 0.0001);
        Assert.assertEquals("5m", duration.value());

        // Test alternate minutes format
        duration = new TimeDuration("5M");
        Assert.assertEquals(5 * 60 * 1000L, duration.getMilliseconds());

        // Test hours
        duration = new TimeDuration("2h");
        Assert.assertEquals(2 * 60 * 60 * 1000L, duration.getMilliseconds());
        Assert.assertEquals(2.0, duration.getHours(), 0.0001);
        Assert.assertEquals("2h", duration.value());

        // Test alternate hours format
        duration = new TimeDuration("2H");
        Assert.assertEquals(2 * 60 * 60 * 1000L, duration.getMilliseconds());

        // Test days
        duration = new TimeDuration("3d");
        Assert.assertEquals(3 * 24 * 60 * 60 * 1000L, duration.getMilliseconds());
        Assert.assertEquals(3.0, duration.getDays(), 0.0001);
        Assert.assertEquals("3d", duration.value());

        // Test alternate days format
        duration = new TimeDuration("3D");
        Assert.assertEquals(3 * 24 * 60 * 60 * 1000L, duration.getMilliseconds());

        // Test weeks
        duration = new TimeDuration("2w");
        Assert.assertEquals(2 * 7 * 24 * 60 * 60 * 1000L, duration.getMilliseconds());
        Assert.assertEquals("2w", duration.value());

        // Test alternate weeks format
        duration = new TimeDuration("2W");
        Assert.assertEquals(2 * 7 * 24 * 60 * 60 * 1000L, duration.getMilliseconds());

        // Test months (approximate - 30 days)
        duration = new TimeDuration("1mo");
        Assert.assertEquals(30 * 24 * 60 * 60 * 1000L, duration.getMilliseconds());
        Assert.assertEquals("1mo", duration.value());

        // Test alternate months format
        duration = new TimeDuration("1MO");
        Assert.assertEquals(30 * 24 * 60 * 60 * 1000L, duration.getMilliseconds());

        // Test years (approximate - 365 days)
        duration = new TimeDuration("1y");
        Assert.assertEquals(365 * 24 * 60 * 60 * 1000L, duration.getMilliseconds());
        Assert.assertEquals("1y", duration.value());

        // Test alternate years format
        duration = new TimeDuration("1Y");
        Assert.assertEquals(365 * 24 * 60 * 60 * 1000L, duration.getMilliseconds());
    }

    @Test
    public void testDecimalInput() {
        // Not supported by the current implementation, but test to document behavior
        try {
            TimeDuration duration = new TimeDuration("1.5h");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected exception
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInput() {
        new TimeDuration("invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new TimeDuration("5x");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyInput() {
        new TimeDuration("");
    }

    @Test
    public void testToJson() {
        TimeDuration duration = new TimeDuration("10s");
        JsonElement json = duration.toJson();
        Assert.assertTrue(json.isJsonObject());

        JsonObject obj = json.getAsJsonObject();
        Assert.assertEquals(TokenType.TIME_DURATION.name(), obj.get("type").getAsString());
        Assert.assertEquals("10s", obj.get("value").getAsString());
        Assert.assertEquals(10 * 1000, obj.get("milliseconds").getAsLong());
    }

    @Test
    public void testTokenType() {
        TimeDuration duration = new TimeDuration("10s");
        Assert.assertEquals(TokenType.TIME_DURATION, duration.type());
    }

    @Test
    public void testWhitespaceInInput() {
        // Test with spaces around number and unit
        TimeDuration duration = new TimeDuration("5 s");
        Assert.assertEquals(5 * 1000L, duration.getMilliseconds());

        duration = new TimeDuration("10 m");
        Assert.assertEquals(10 * 60 * 1000L, duration.getMilliseconds());
    }

    @Test
    public void testNumericValues() {
        // Test millisecond conversion accuracy for common time units

        // 1 second = 1000 milliseconds
        TimeDuration duration = new TimeDuration("1s");
        Assert.assertEquals(TimeUnit.SECONDS.toMillis(1), duration.getMilliseconds());

        // 1 minute = 60,000 milliseconds
        duration = new TimeDuration("1m");
        Assert.assertEquals(TimeUnit.MINUTES.toMillis(1), duration.getMilliseconds());

        // 1 hour = 3,600,000 milliseconds
        duration = new TimeDuration("1h");
        Assert.assertEquals(TimeUnit.HOURS.toMillis(1), duration.getMilliseconds());

        // 1 day = 86,400,000 milliseconds
        duration = new TimeDuration("1d");
        Assert.assertEquals(TimeUnit.DAYS.toMillis(1), duration.getMilliseconds());

        // Test conversion methods
        duration = new TimeDuration("60s");
        Assert.assertEquals(60.0, duration.getSeconds(), 0.0001);
        Assert.assertEquals(1.0, duration.getMinutes(), 0.0001);

        duration = new TimeDuration("120m");
        Assert.assertEquals(120.0, duration.getMinutes(), 0.0001);
        Assert.assertEquals(2.0, duration.getHours(), 0.0001);

        duration = new TimeDuration("48h");
        Assert.assertEquals(48.0, duration.getHours(), 0.0001);
        Assert.assertEquals(2.0, duration.getDays(), 0.0001);
    }

    @Test
    public void testLargeValues() {
        // Test large values
        TimeDuration duration = new TimeDuration("999999s");
        Assert.assertEquals(999999 * 1000L, duration.getMilliseconds());

        // Testing zero value
        duration = new TimeDuration("0s");
        Assert.assertEquals(0L, duration.getMilliseconds());
    }
}