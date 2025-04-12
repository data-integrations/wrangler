/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */
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
