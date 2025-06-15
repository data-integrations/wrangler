package io.cdap.wrangler.api.parser;

import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

    @Test
    public void testParseNanos() {
        TimeDuration duration = new TimeDuration("1000ns");
        Assert.assertEquals(1000L, duration.getNanoseconds());
    }

    @Test
    public void testParseMicros() {
        TimeDuration duration = new TimeDuration("500us");
        Assert.assertEquals(500 * 1000L, duration.getNanoseconds());
    }

    @Test
    public void testParseMillis() {
        TimeDuration duration = new TimeDuration("5ms");
        Assert.assertEquals(5 * 1000 * 1000L, duration.getNanoseconds());
    }

    @Test
    public void testParseSeconds() {
        TimeDuration duration = new TimeDuration("2.1s");
        Assert.assertEquals((long)(2.1 * 1000 * 1000 * 1000), duration.getNanoseconds());
    }

    @Test
    public void testParseMinutes() {
        TimeDuration duration = new TimeDuration("1.5m");
        Assert.assertEquals((long)(1.5 * 60 * 1000 * 1000 * 1000), duration.getNanoseconds());
    }

    @Test
    public void testParseHours() {
        TimeDuration duration = new TimeDuration("0.5h");
        Assert.assertEquals((long)(0.5 * 60 * 60 * 1000 * 1000 * 1000), duration.getNanoseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFormat() {
        new TimeDuration("invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeValue() {
        new TimeDuration("-1s");
    }

    @Test
    public void testToString() {
        TimeDuration duration = new TimeDuration("1s");
        Assert.assertEquals("1s", duration.toString());
    }
} 