package io.cdap.wrangler.api.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for ByteSize and TimeDuration classes.
 */
public class ByteSizeAndTimeDurationTest {

    @Test
    public void testByteSizeParsing() {
        // Test parsing valid inputs to canonical byte units
        Assert.assertEquals(1024L, new ByteSize("1KB").getBytes());
        Assert.assertEquals(1048576L, new ByteSize("1MB").getBytes());
        Assert.assertEquals(1073741824L, new ByteSize("1GB").getBytes());
        Assert.assertEquals(10, new ByteSize("10B").getBytes());

        // Test for case insensitivity
        Assert.assertEquals(1572864L, new ByteSize("1.5MB").getBytes());

    }

    @Test
    public void testTimeDurationParsing() {
        // Test parsing valid inputs to canonical time units
        // Assuming TimeDuration.getValue() returns nanoseconds

        // 5ms -> 5 * 1,000,000 = 5,000,000 ns. Correct.
        Assert.assertEquals(5000000L, new TimeDuration("5ms").getValue());

        // 2.1s -> 2.1 * 1,000,000,000 = 2,100,000,000 ns. Correct.
        Assert.assertEquals(2100000000L, new TimeDuration("2.1s").getValue());

        // 1h -> 1 * 60 * 60 * 1,000,000,000 = 3,600,000,000,000 ns. Correct.
        Assert.assertEquals(3600000000000L, new TimeDuration("1h").getValue());

        // Test for case insensitivity (using "min")
        // 1.5min -> 1.5 * 60 * 1,000,000,000 = 90,000,000,000 ns. Correct.
        Assert.assertEquals(90000000000L, new  TimeDuration("1.5min").getValue());

    }
}
