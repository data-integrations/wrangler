package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testByteSize() {
        ByteSize size = new ByteSize("10MB");
        Assert.assertEquals(10 * 1024 * 1024, size.getBytes());
    }

    @Test
    public void testDifferentUnits() {
        Assert.assertEquals(1024, new ByteSize("1KB").getBytes());
        Assert.assertEquals(1, new ByteSize("1B").getBytes());
        Assert.assertEquals(1024L * 1024 * 1024, new ByteSize("1GB").getBytes());
    }
}
