package io.cdap.wrangler.api.parser;

import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

    @Test
    public void testMilliseconds() {
        TimeDuration duration = new TimeDuration("150ms");
        Assert.assertEquals(150, duration.getMilliseconds());
    }

    @Test
    public void testSeconds() {
        TimeDuration duration = new TimeDuration("2s");
        Assert.assertEquals(2000, duration.getMilliseconds());
    }

    @Test
    public void testMinutes() {
        TimeDuration duration = new TimeDuration("3m");
        Assert.assertEquals(3 * 60 * 1000, duration.getMilliseconds());
    }

    @Test
    public void testHours() {
        TimeDuration duration = new TimeDuration("1h");
        Assert.assertEquals(1 * 60 * 60 * 1000, duration.getMilliseconds());
    }

    @Test
    public void testDefaultMilliseconds() {
        TimeDuration duration = new TimeDuration("500");
        Assert.assertEquals(500, duration.getMilliseconds());  // default is ms
    }

    @Test(expected = NumberFormatException.class)
    public void testInvalidFormat() {
        new TimeDuration("abc");  // yeh throw karega exception
    }
}
