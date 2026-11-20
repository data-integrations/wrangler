package io.cdap.wrangler.api.parser;

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testKB() {
        ByteSize size = new ByteSize("10KB");
        Assert.assertEquals(10240, size.getBytes()); // 10 * 1024
    }

    @Test
    public void testMB() {
        ByteSize size = new ByteSize("2MB");
        Assert.assertEquals(2097152, size.getBytes()); // 2 * 1024 * 1024
    }

    @Test
    public void testGB() {
        ByteSize size = new ByteSize("1GB");
        Assert.assertEquals(1073741824, size.getBytes()); // 1 * 1024 * 1024 * 1024
    }

    @Test
    public void testBytesOnly() {
        ByteSize size = new ByteSize("500");
        Assert.assertEquals(500, size.getBytes());
    }

    @Test
    public void testJsonOutput() {
        ByteSize size = new ByteSize("1KB");
        Assert.assertEquals(1024, size.toJson().getAsJsonObject().get("value").getAsLong());
    }
}
