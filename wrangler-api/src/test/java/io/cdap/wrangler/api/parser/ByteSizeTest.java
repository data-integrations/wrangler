package io.cdap.wrangler.api.parser;

import org.junit.Test;
import static org.junit.Assert.*;

public class ByteSizeTest {

    @Test
    public void testParsing() {
        ByteSize size1 = new ByteSize("10B");
        assertEquals(10, size1.getBytes());

        ByteSize size2 = new ByteSize("2KB");
        assertEquals(2048, size2.getBytes());

        ByteSize size3 = new ByteSize("1.5MB");
        assertEquals(1.5 * 1024 * 1024, size3.getBytes(), 0.01);

        ByteSize size4 = new ByteSize("3GB");
        assertEquals(3L * 1024 * 1024 * 1024, size4.getBytes());

        ByteSize size5 = new ByteSize("0.5TB");
        assertEquals(0.5 * 1024 * 1024 * 1024 * 1024, size5.getBytes(), 0.01);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new ByteSize("100XYZ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMalformedInput() {
        new ByteSize("ABC");
    }
}
