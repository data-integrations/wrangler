package io.cdap.wrangler;

import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testBytes() {
        ByteSize b = new ByteSize("100B");
        Assert.assertEquals(100, b.getBytes());
    }

    @Test
    public void testKB() {
        ByteSize b = new ByteSize("1KB");
        Assert.assertEquals(1024, b.getBytes());
    }

    @Test
    public void testMB() {
        ByteSize b = new ByteSize("1.5MB");
        Assert.assertEquals(1572864, b.getBytes());
    }

    @Test
    public void testGB() {
        ByteSize b = new ByteSize("2GB");
        Assert.assertEquals(2L * 1024 * 1024 * 1024, b.getBytes());
    }

    @Test
    public void testTB() {
        ByteSize b = new ByteSize("1TB");
        Assert.assertEquals(1L * 1024 * 1024 * 1024 * 1024, b.getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFormat() {
        new ByteSize("15XY");
    }
}
