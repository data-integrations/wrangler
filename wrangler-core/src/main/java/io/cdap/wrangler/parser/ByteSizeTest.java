package io.cdap.wrangler.parser;

import com.google.gson.JsonObject;

import io.cdap.wrangler.api.parser.ByteSize;

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testByteUnits() {
        Assert.assertEquals(1L, new ByteSize("1b").getBytes());
        Assert.assertEquals(1024L, new ByteSize("1kb").getBytes());
        Assert.assertEquals(1024L * 1024, new ByteSize("1mb").getBytes());
        Assert.assertEquals(1024L * 1024 * 1024, new ByteSize("1gb").getBytes());
        Assert.assertEquals(1024L * 1024 * 1024 * 1024, new ByteSize("1tb").getBytes());
    }

    @Test
    public void testShorthandUnits() {
        Assert.assertEquals(1024L, new ByteSize("1k").getBytes());
        Assert.assertEquals(1024L * 1024, new ByteSize("1m").getBytes());
        Assert.assertEquals(1024L * 1024 * 1024, new ByteSize("1g").getBytes());
    }

    @Test
    public void testDefaultToBytes() {
        Assert.assertEquals(123, new ByteSize("123").getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInput() {
        new ByteSize("k1");
    }

    @Test
    public void testToJson() {
        ByteSize size = new ByteSize("1kb");
        JsonObject json = size.toJson().getAsJsonObject();
        Assert.assertEquals("BYTE_SIZE", json.get("type").getAsString());
        Assert.assertEquals(1024L, json.get("value").getAsLong());
        Assert.assertEquals("1kb", json.get("original").getAsString());
    }
}