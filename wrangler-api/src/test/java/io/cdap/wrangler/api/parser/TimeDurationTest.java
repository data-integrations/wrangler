package io.cdap.wrangler.api.parser;

import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

    @Test
    public void testParseTimeDuration() {
        Assert.assertEquals(5, TimeDuration.parse("5ms").getMilliseconds());
        Assert.assertEquals(2100, TimeDuration.parse("2.1s").getMilliseconds());
        Assert.assertEquals(60000, TimeDuration.parse("1m").getMilliseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDuration() {
        TimeDuration.parse("invalid");
    }
}