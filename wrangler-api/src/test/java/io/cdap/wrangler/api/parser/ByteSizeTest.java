/*
 * Copyright © 2017-2019 Cask Data, Inc.
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
 * Tests for the ByteSize class.
 */
public class ByteSizeTest {

    @Test
    public void testParseByteSize() {
        // Test basic byte sizes
        ByteSize byteSize1 = new ByteSize("1024");
        Assert.assertEquals(1024L, byteSize1.getBytes());
        Assert.assertEquals("1024", byteSize1.getOriginalValue());
        Assert.assertEquals(TokenType.BYTE_SIZE, byteSize1.type());
        Assert.assertEquals(1024L, byteSize1.value().longValue());

        // Test with KB
        ByteSize byteSize2 = new ByteSize("1KB");
        Assert.assertEquals(1024L, byteSize2.getBytes());
        Assert.assertEquals("1KB", byteSize2.getOriginalValue());

        // Test with MB
        ByteSize byteSize3 = new ByteSize("1MB");
        Assert.assertEquals(1024L * 1024L, byteSize3.getBytes());
        Assert.assertEquals("1MB", byteSize3.getOriginalValue());

        // Test with GB
        ByteSize byteSize4 = new ByteSize("1GB");
        Assert.assertEquals(1024L * 1024L * 1024L, byteSize4.getBytes());
        Assert.assertEquals("1GB", byteSize4.getOriginalValue());

        // Test with TB
        ByteSize byteSize5 = new ByteSize("1TB");
        Assert.assertEquals(1024L * 1024L * 1024L * 1024L, byteSize5.getBytes());
        Assert.assertEquals("1TB", byteSize5.getOriginalValue());

        // Test with decimal values
        ByteSize byteSize6 = new ByteSize("1.5MB");
        Assert.assertEquals(1024L * 1024L * 3 / 2, byteSize6.getBytes());
        Assert.assertEquals("1.5MB", byteSize6.getOriginalValue());

        // Test with lowercase units
        ByteSize byteSize7 = new ByteSize("1kb");
        Assert.assertEquals(1024L, byteSize7.getBytes());
        Assert.assertEquals("1kb", byteSize7.getOriginalValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize() {
        new ByteSize("invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyByteSize() {
        new ByteSize("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullByteSize() {
        new ByteSize(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedUnit() {
        new ByteSize("1PB");
    }

    @Test
    public void testToJson() {
        ByteSize byteSize = new ByteSize("1MB");
        JsonElement json = byteSize.toJson();

        Assert.assertTrue(json.isJsonObject());
        JsonObject jsonObject = json.getAsJsonObject();

        Assert.assertEquals(TokenType.BYTE_SIZE.name(), jsonObject.get("type").getAsString());
        Assert.assertEquals("1MB", jsonObject.get("value").getAsString());
        Assert.assertEquals(1024L * 1024L, jsonObject.get("bytes").getAsLong());
    }
}