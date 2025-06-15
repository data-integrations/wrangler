package io.cdap.wrangler.api.parser;

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testParseBytes() {
        ByteSize size = new ByteSize("1024B");
        Assert.assertEquals(1024L, size.getBytes());
    }

    @Test
    public void testParseKB() {
        ByteSize size = new ByteSize("10kb");
        Assert.assertEquals(10 * 1024L, size.getBytes());
    }

    @Test
    public void testParseMB() {
        ByteSize size = new ByteSize("1.5MB");
        Assert.assertEquals((long)(1.5 * 1024 * 1024), size.getBytes());
    }

    @Test
    public void testParseGB() {
        ByteSize size = new ByteSize("2GB");
        Assert.assertEquals(2L * 1024 * 1024 * 1024, size.getBytes());
    }

    @Test
    public void testParseTB() {
        ByteSize size = new ByteSize("0.5TB");
        Assert.assertEquals((long)(0.5 * 1024 * 1024 * 1024 * 1024), size.getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFormat() {
        new ByteSize("invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeValue() {
        new ByteSize("-1MB");
    }

    @Test
    public void testToString() {
        ByteSize size = new ByteSize("1MB");
        Assert.assertEquals("1MB", size.toString());
    }
} 