package io.cdap.wrangler;

import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

    @Test
    public void testMilliseconds() {
        TimeDuration t = new TimeDuration("500ms");
        Assert.assertEquals(500, t.getMilliseconds());
    }

    @Test
    public void testSeconds() {
        TimeDuration t = new TimeDuration("2s");
        Assert.assertEquals(2000, t.getMilliseconds());
    }

    @Test
    public void testMinutes() {
        TimeDuration t = new TimeDuration("1.5m");
        Assert.assertEquals(90000, t.getMilliseconds());
    }

    @Test
    public void testHours() {
        TimeDuration t = new TimeDuration("1h");
        Assert.assertEquals(3600000, t.getMilliseconds());
    }

    @Test
    public void testDays() {
        TimeDuration t = new TimeDuration("2d");
        Assert.assertEquals(2L * 24 * 60 * 60 * 1000, t.getMilliseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDuration() {
        new TimeDuration("10xyz");
    }
}
