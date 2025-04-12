package io.cdap.wrangler.parser;

import com.google.gson.JsonElement;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

    @Test
    public void testTimeDurationConversion() {
        TimeDuration timeDuration = new TimeDuration("10s");

        // Test conversion from string to nanoseconds
        Assert.assertEquals(10000000000L, timeDuration.getNanos());  // 10 * 10^9

        timeDuration = new TimeDuration("1.5m");
        Assert.assertEquals(90000000000L, timeDuration.getNanos()); // 1.5 * 60 * 10^9

        timeDuration = new TimeDuration("2h");
        Assert.assertEquals(7200000000000L, timeDuration.getNanos());  // 2 * 60 * 60 * 10^9

        timeDuration = new TimeDuration("5d");
        Assert.assertEquals(432000000000000L, timeDuration.getNanos());  // 5 * 24 * 60 * 60 * 10^9

        // Test edge case like 0
        timeDuration = new TimeDuration("0s");
        Assert.assertEquals(0, timeDuration.getNanos());
    }

    @Test
    public void testInvalidTimeDuration() {
        // Test invalid time duration values
        try {
            new TimeDuration("10XZ");
            Assert.fail("Expected IllegalArgumentException for invalid unit");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Invalid time duration format '10XZ'. Expected format is <number><unit> where unit is ms, s, m, h, or d", e.getMessage());
        }
    }

    @Test
    public void testToStringRepresentation() {
        TimeDuration timeDuration = new TimeDuration("3600s");
        Assert.assertEquals("3600s", timeDuration.getOriginal());
    }

    @Test
    public void testJsonSerialization() {
        TimeDuration timeDuration = new TimeDuration("2h");
        JsonElement json = timeDuration.toJson();

        // Validate the JSON structure
        Assert.assertEquals("TIME_DURATION", json.getAsJsonObject().get("type").getAsString());
        Assert.assertEquals("2h", json.getAsJsonObject().get("value").getAsString());
        Assert.assertEquals(7200000000000L, json.getAsJsonObject().get("nanoseconds").getAsLong()); // 2 * 60 * 60 * 10^9
    }
}
