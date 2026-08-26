package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTimeDurationTest {

    @Test
    public void testByteSizeParsing() {
        Assert.assertEquals(1024L, new ByteSize("1KB").getBytes());
        Assert.assertEquals(1_048_576L, new ByteSize("1MB").getBytes());
        Assert.assertEquals(1_073_741_824L, new ByteSize("1GB").getBytes());
        Assert.assertEquals(1_099_511_627_776L, new ByteSize("1TB").getBytes());
    }

    @Test
    public void testTimeDurationParsing() {
        Assert.assertEquals(5_000_000L, new TimeDuration("5ms").getMillis());
        Assert.assertEquals(60_000_000_000L, new TimeDuration("1m").getMillis());
        Assert.assertEquals(3_600_000_000_000L, new TimeDuration("1h").getMillis());
    }
}