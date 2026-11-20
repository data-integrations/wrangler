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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link ByteSize} token.
 */
public class ByteSizeTest {
    @Test
    public void testParseBytes() {
        // Test basic units
        Assert.assertEquals(1L, new ByteSize("1B").getBytes());
        Assert.assertEquals(1024L, new ByteSize("1KB").getBytes());
        Assert.assertEquals(1024L * 1024L, new ByteSize("1MB").getBytes());
        Assert.assertEquals(1024L * 1024L * 1024L, new ByteSize("1GB").getBytes());
        Assert.assertEquals(1024L * 1024L * 1024L * 1024L, new ByteSize("1TB").getBytes());
        Assert.assertEquals(1024L * 1024L * 1024L * 1024L * 1024L, new ByteSize("1PB").getBytes());

        // Test decimal values
        Assert.assertEquals(512L, new ByteSize("0.5KB").getBytes());
        Assert.assertEquals(1536L, new ByteSize("1.5KB").getBytes());
        Assert.assertEquals(1024L * 1024L * 1.5, new ByteSize("1.5MB").getBytes(), 0.001);

        // Test case insensitivity
        Assert.assertEquals(1024L, new ByteSize("1kb").getBytes());
        Assert.assertEquals(1024L, new ByteSize("1Kb").getBytes());
        Assert.assertEquals(1024L, new ByteSize("1kB").getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new ByteSize("1XB");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFormat() {
        new ByteSize("KB");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNumber() {
        new ByteSize("abcKB");
    }
}
