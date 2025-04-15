package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenException;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteSizeAndTimeDurationTest {

    @Test
    public void testByteSize() throws TokenException {
        // Test decimal units
        ByteSize kb = new ByteSize("10KB", 1, 0);
        assertEquals(10 * 1024L, kb.getBytes());

        ByteSize mb = new ByteSize("5MB", 1, 0);
        assertEquals(5 * 1024 * 1024L, mb.getBytes());

        ByteSize gb = new ByteSize("2GB", 1, 0);
        assertEquals(2 * 1024 * 1024 * 1024L, gb.getBytes());

        // Test different formats
        ByteSize justB = new ByteSize("1024B", 1, 0);
        assertEquals(1024L, justB.getBytes());

        ByteSize lowerCase = new ByteSize("10kb", 1, 0);
        assertEquals(10 * 1024L, lowerCase.getBytes());

        ByteSize byteSize = new ByteSize("1024MB", 1, 0);
        assertEquals("1024.00MB", byteSize.toString("MB")); // Corrected for precision
    }

    @Test
    public void testTimeDuration() throws TokenException {
        // Test milliseconds
        TimeDuration ms = new TimeDuration("150ms", 1, 0);
        assertEquals(150 * 1000000L, ms.getNanoseconds());

        // Test seconds
        TimeDuration sec = new TimeDuration("5s", 1, 0);
        assertEquals(5 * 1000000000L, sec.getNanoseconds());

        // Test minutes
        TimeDuration min = new TimeDuration("3min", 1, 0);
        assertEquals(3 * 60 * 1000000000L, min.getNanoseconds());

        // Test days
        TimeDuration day = new TimeDuration("1d", 1, 0);
        assertEquals(24 * 60 * 60 * 1000000000L, day.getNanoseconds());

        // Test microseconds
        TimeDuration us = new TimeDuration("500us", 1, 0);
        assertEquals(500 * 1000L, us.getNanoseconds());

        // Test nanoseconds
        TimeDuration ns = new TimeDuration("800ns", 1, 0);
        assertEquals(800L, ns.getNanoseconds());
    }

    @Test
    public void testByteSizeConversion() throws TokenException {
        ByteSize original = new ByteSize("1024KB", 1, 0);

        // Test converting to MB (expecting 1MB)
        assertEquals("1.00MB", original.toString("MB"));  // Corrected precision

        // Test converting to GB (expecting 0.00GB)
        assertEquals("0.00GB", original.toString("GB"));

        // Test converting to KiB (expecting 1024KiB)
        assertEquals("1024.00KiB", original.toString("KiB")); // Corrected precision
    }

    @Test
    public void testTimeDurationConversion() throws TokenException {
        TimeDuration original = new TimeDuration("5000ms", 1, 0);

        // Test converting to seconds (expecting 5.00s)
        assertEquals("5.00s", original.toString("s"));  // Corrected precision

        // Test converting to minutes (expecting 0.08min)
        assertEquals("0.08min", original.toString("min"));

        // Test converting to microseconds (expecting 5000000.00us)
        assertEquals("5000000.00us", original.toString("us")); // Corrected precision
    }
}
