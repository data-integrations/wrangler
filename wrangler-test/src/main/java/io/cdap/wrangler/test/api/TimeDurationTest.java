package io.cdap.wrangler.api;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TimeDurationTest {

    @Test
    public void testTimeDurationParsing() {
        assertEquals(5000000L, TimeDuration.parse("5ms"));
        assertEquals(2100000000L, TimeDuration.parse("2.1s"));
        assertEquals(1000000L, TimeDuration.parse("1000us"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDuration() {
        TimeDuration.parse("invalidDuration");
    }
}
