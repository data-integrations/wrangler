package io.cdap.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testByteSizeParsing() {
        ByteSize byteSize1 = new ByteSize("10kb");
        Assert.assertEquals(10240, byteSize1.getBytes());

        ByteSize byteSize2 = new ByteSize("1.5MB");
        Assert.assertEquals(1572864, byteSize2.getBytes());

        ByteSize byteSize3 = new ByteSize("5GB");
        Assert.assertEquals(5368709120L, byteSize3.getBytes());
    }

    @Test(expected = NumberFormatException.class)
    public void testInvalidByteSizeParsing() {
        new ByteSize("invalid");
    }
}
