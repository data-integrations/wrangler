package io.cdap.wrangler.api;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ByteSizeTest {

    @Test
    public void testByteSizeParsing() {
        assertEquals(10240, ByteSize.parse("10kb"));
        assertEquals(1572864, ByteSize.parse("1.5MB"));
        assertEquals(5368709120L, ByteSize.parse("5GB"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize() {
        ByteSize.parse("invalidSize");
    }
}
