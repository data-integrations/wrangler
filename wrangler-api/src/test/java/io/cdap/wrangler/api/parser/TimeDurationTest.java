package io.cdap.wrangler.api.parser;

import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

    @Test
    public void testDuration() throws TokenException {
        TimeDuration duration1 = new TimeDuration("10ns", 1, 0);
        Assert.assertEquals(10, duration1.getNanoseconds());

        TimeDuration duration2 = new TimeDuration("10us", 1, 0);
        Assert.assertEquals(10 * 1000, duration2.getNanoseconds());

        TimeDuration duration3 = new TimeDuration("10ms", 1, 0);
        Assert.assertEquals(10 * 1000 * 1000, duration3.getNanoseconds());

        TimeDuration duration4 = new TimeDuration("10s", 1, 0);
        Assert.assertEquals(10_000_000_000L, duration4.getNanoseconds());

        TimeDuration duration5 = new TimeDuration("10m", 1, 0);
        Assert.assertEquals(10 * 60 * 1000 * 1000 * 1000L, duration5.getNanoseconds());

        TimeDuration duration6 = new TimeDuration("10h", 1, 0);
        Assert.assertEquals(36_000_000_000_000L, duration6.getNanoseconds());

        // Add test for days
        TimeDuration duration7 = new TimeDuration("10d", 1, 0);
        Assert.assertEquals(864_000_000_000_000L, duration7.getNanoseconds());
    }

    @Test(expected = TokenException.class)
    public void testInvalidDuration1() throws TokenException {
        new TimeDuration("10", 1, 0); // Missing unit
    }

    @Test(expected = TokenException.class)
    public void testInvalidDuration2() throws TokenException {
        new TimeDuration("10xs", 1, 0); // Invalid unit
    }
}