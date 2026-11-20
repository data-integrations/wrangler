/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ByteSize} token implementation.
 */
public class ByteSizeTest {

    @Test
    public void testValidByteSizeParsing() {
        // Test bytes
        ByteSize size = new ByteSize("10B");
        Assert.assertEquals(10L, size.getBytes());
        Assert.assertEquals("10B", size.value());

        // Test kilobytes
        size = new ByteSize("5KB");
        Assert.assertEquals(5 * 1024L, size.getBytes());
        Assert.assertEquals(5.0, size.getKilobytes(), 0.0001);
        Assert.assertEquals("5KB", size.value());

        // Test alternate kilobyte formats
        size = new ByteSize("5K");
        Assert.assertEquals(5 * 1024L, size.getBytes());

        size = new ByteSize("5kb");
        Assert.assertEquals(5 * 1024L, size.getBytes());

        // Test megabytes
        size = new ByteSize("2MB");
        Assert.assertEquals(2 * 1024 * 1024L, size.getBytes());
        Assert.assertEquals(2.0, size.getMegabytes(), 0.0001);
        Assert.assertEquals("2MB", size.value());

        // Test alternate megabyte formats
        size = new ByteSize("2M");
        Assert.assertEquals(2 * 1024 * 1024L, size.getBytes());

        size = new ByteSize("2mb");
        Assert.assertEquals(2 * 1024 * 1024L, size.getBytes());

        // Test gigabytes
        size = new ByteSize("1GB");
        Assert.assertEquals(1 * 1024 * 1024 * 1024L, size.getBytes());
        Assert.assertEquals(1.0, size.getGigabytes(), 0.0001);
        Assert.assertEquals("1GB", size.value());

        // Test alternate gigabyte formats
        size = new ByteSize("1G");
        Assert.assertEquals(1 * 1024 * 1024 * 1024L, size.getBytes());

        size = new ByteSize("1gb");
        Assert.assertEquals(1 * 1024 * 1024 * 1024L, size.getBytes());

        // Test terabytes
        size = new ByteSize("3TB");
        Assert.assertEquals(3L * 1024 * 1024 * 1024 * 1024, size.getBytes());
        Assert.assertEquals("3TB", size.value());

        // Test petabytes (large values)
        size = new ByteSize("2PB");
        Assert.assertEquals(2L * 1024 * 1024 * 1024 * 1024 * 1024, size.getBytes());
        Assert.assertEquals("2PB", size.value());
    }

    @Test
    public void testDecimalInput() {
        // Not supported by the current implementation, but test to document behavior
        try {
            ByteSize size = new ByteSize("1.5MB");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected exception
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidInput() {
        new ByteSize("invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new ByteSize("5XB");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyInput() {
        new ByteSize("");
    }

    @Test
    public void testToJson() {
        ByteSize size = new ByteSize("10KB");
        JsonElement json = size.toJson();
        Assert.assertTrue(json.isJsonObject());

        JsonObject obj = json.getAsJsonObject();
        Assert.assertEquals(TokenType.BYTE_SIZE.name(), obj.get("type").getAsString());
        Assert.assertEquals("10KB", obj.get("value").getAsString());
        Assert.assertEquals(10 * 1024, obj.get("bytes").getAsLong());
    }

    @Test
    public void testTokenType() {
        ByteSize size = new ByteSize("10KB");
        Assert.assertEquals(TokenType.BYTE_SIZE, size.type());
    }

    @Test
    public void testWhitespaceInInput() {
        // Test with spaces around number and unit
        ByteSize size = new ByteSize("5 KB");
        Assert.assertEquals(5 * 1024L, size.getBytes());

        size = new ByteSize("10 MB");
        Assert.assertEquals(10 * 1024 * 1024L, size.getBytes());
    }

    @Test
    public void testNumericBaseValues() {
        // Test simple byte values
        ByteSize size = new ByteSize("0B");
        Assert.assertEquals(0L, size.getBytes());

        size = new ByteSize("1024B");
        Assert.assertEquals(1024L, size.getBytes());
        Assert.assertEquals(1.0, size.getKilobytes(), 0.0001);

        // Test large values
        size = new ByteSize("9999999KB");
        Assert.assertEquals(9999999L * 1024, size.getBytes());

        // Test with no unit (should default to bytes)
        size = new ByteSize("500");
        Assert.assertEquals(500L, size.getBytes());
    }
}