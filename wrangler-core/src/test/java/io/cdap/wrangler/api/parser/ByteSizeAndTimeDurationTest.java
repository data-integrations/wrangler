package io.cdap.wrangler.api.parser;

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeAndTimeDurationTest {

    @Test
    public void testByteSizeParsing() {
        ByteSize byteSize1 = new ByteSize("10KB");
        Assert.assertEquals(10 * 1024, byteSize1.getBytes());

        ByteSize byteSize2 = new ByteSize("1.5MB");
        Assert.assertEquals((long) (1.5 * 1024 * 1024), byteSize2.getBytes());

        ByteSize byteSize3 = new ByteSize("2GB");
        Assert.assertEquals(2L * 1024 * 1024 * 1024, byteSize3.getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSizeParsing() {
        new ByteSize("10XYZ");
    }

    @Test
    public void testTimeDurationParsing() {
        TimeDuration timeDuration1 = new TimeDuration("5ms");
        Assert.assertEquals(5, timeDuration1.getMilliseconds());

        TimeDuration timeDuration2 = new TimeDuration("2.1s");
        Assert.assertEquals((long) (2.1 * 1000), timeDuration2.getMilliseconds());

        TimeDuration timeDuration3 = new TimeDuration("3min");
        Assert.assertEquals(3 * 60 * 1000, timeDuration3.getMilliseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDurationParsing() {
        new TimeDuration("abc");
    }
}