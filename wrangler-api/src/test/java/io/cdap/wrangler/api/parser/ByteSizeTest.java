package io.cdap.wrangler.api.parser;

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testParseByteSize() {
        Assert.assertEquals(10240, ByteSize.parse("10KB").getBytes());
        Assert.assertEquals(1572864, ByteSize.parse("1.5MB").getBytes());
        Assert.assertEquals(5368709120L, ByteSize.parse("5GB").getBytes());
        Assert.assertEquals(1024, ByteSize.parse("1KB").getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize() {
        ByteSize.parse("invalid");
    }
}