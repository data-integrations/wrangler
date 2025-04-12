package io.cdap.wrangler.api.parser;

import org.junit.Test;
import static org.junit.Assert.*;

public class ByteSizeAndTimeDurationTest {

    @Test
    public void testByteSizeParsing() {
        assertEquals(10240L, new ByteSize("10KB").getBytes());
        assertEquals(1572864L, new ByteSize("1.5MB").getBytes()); // 1.5 * 1024 * 1024
        assertEquals(1073741824L, new ByteSize("1GB").getBytes());
        assertEquals(1L, new ByteSize("1B").getBytes());
        assertEquals(1099511627776L, new ByteSize("1TB").getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize() {
        new ByteSize("10XY");
    }

    @Test
    public void testTimeDurationParsing() {
        assertEquals(5L, new TimeDuration("5ms").getMilliseconds());
        assertEquals(2100L, new TimeDuration("2.1s").getMilliseconds());
        assertEquals(60000L, new TimeDuration("1m").getMilliseconds());
        assertEquals(3600000L, new TimeDuration("1h").getMilliseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDuration() {
        new TimeDuration("12xy");
    }
}