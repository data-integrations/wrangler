package io.cdap.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

    @Test
    public void testTimeDurationParsing() {
        TimeDuration timeDuration1 = new TimeDuration("5ms");
        Assert.assertEquals(5, timeDuration1.getMilliseconds());

        TimeDuration timeDuration2 = new TimeDuration("2.1s");
        Assert.assertEquals(2100, timeDuration2.getMilliseconds());

        TimeDuration timeDuration3 = new TimeDuration("3h");
        Assert.assertEquals(10800000, timeDuration3.getMilliseconds());
    }

    @Test(expected = NumberFormatException.class)
    public void testInvalidTimeDurationParsing() {
        new TimeDuration("invalid");
    }
}
