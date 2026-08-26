package io.cdap.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeParserTest {

    @Test
    public void testValidInputs() {
        Assert.assertEquals(1024L, ByteSizeParser.parse("1KB"));
        Assert.assertEquals(1048576L, ByteSizeParser.parse("1MB"));
        Assert.assertEquals(1073741824L, ByteSizeParser.parse("1GB"));
        Assert.assertEquals(1099511627776L, ByteSizeParser.parse("1TB"));
        Assert.assertEquals(1125899906842624L, ByteSizeParser.parse("1PB"));
        Assert.assertEquals(123L, ByteSizeParser.parse("123B"));
        Assert.assertEquals(2048L, ByteSizeParser.parse("2 KB"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFormat() {
        ByteSizeParser.parse("10XB");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingUnit() {
        ByteSizeParser.parse("100");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyString() {
        ByteSizeParser.parse("");
    }
}
