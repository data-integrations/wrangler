package io.cdap.wrangler.api.parser;

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testBytes() throws Exception {
        // Test ByteSize's constructor that requires line and column info
        ByteSize size1 = new ByteSize("10B", 1, 0);
        Assert.assertEquals(10, size1.getBytes());
        Assert.assertEquals("B", size1.getUnit());

        ByteSize size2 = new ByteSize("10KB", 1, 0);
        Assert.assertEquals(10 * 1024, size2.getBytes());
        Assert.assertEquals("KB", size2.getUnit());

        ByteSize size3 = new ByteSize("10MB", 1, 0);
        Assert.assertEquals(10 * 1024 * 1024, size3.getBytes());
        Assert.assertEquals("MB", size3.getUnit());

        ByteSize size4 = new ByteSize("10GB", 1, 0);
        Assert.assertEquals(10L * 1024 * 1024 * 1024, size4.getBytes());
        Assert.assertEquals("GB", size4.getUnit());

        ByteSize size5 = new ByteSize("10TB", 1, 0);
        Assert.assertEquals(10L * 1024 * 1024 * 1024 * 1024, size5.getBytes());
        Assert.assertEquals("TB", size5.getUnit());
    }

    @Test(expected = TokenException.class)
    public void testInvalidBytes1() throws Exception {
        new ByteSize("10", 1, 0); // Missing unit
    }

    @Test(expected = TokenException.class)
    public void testInvalidBytes2() throws Exception {
        new ByteSize("10XB", 1, 0); // Invalid unit
    }
}