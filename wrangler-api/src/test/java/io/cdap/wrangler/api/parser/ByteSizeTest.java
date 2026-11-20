/*
 * Copyright © 2025 CDAP
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

import io.cdap.wrangler.api.DirectiveParseException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ByteSize}
 */
public class ByteSizeTest {

    @Test
    public void testValidByteSizes() throws DirectiveParseException {
        Assert.assertEquals(1024L, new ByteSize("1KB").getBytes());
        Assert.assertEquals(1024L, new ByteSize(" 1KB ").getBytes()); // Test spaces around
        Assert.assertEquals(1024L, new ByteSize("1kb").getBytes()); // Test lowercase
        Assert.assertEquals(1024L * 1024L, new ByteSize("1MB").getBytes());
        Assert.assertEquals(1024L * 1024L, new ByteSize("1M").getBytes()); // Test without B
        Assert.assertEquals(1024L * 1024L * 1024L, new ByteSize("1GB").getBytes());
        Assert.assertEquals(1024L * 1024L * 1024L * 1024L, new ByteSize("1TB").getBytes());
        Assert.assertEquals(1024L * 1024L * 1024L * 1024L * 1024L, new ByteSize("1PB").getBytes());
        Assert.assertEquals(0L, new ByteSize("0KB").getBytes());
        Assert.assertEquals(1536L, new ByteSize("1.5KB").getBytes()); // Test double value (1.5 * 1024)
        Assert.assertEquals(10L, new ByteSize("10").getBytes()); // Test plain bytes
        Assert.assertEquals(10L, new ByteSize("10B").getBytes()); // Test plain bytes with B
        Assert.assertEquals(10L, new ByteSize(" 10 b ").getBytes()); // Test spaces and lowercase b
    }

    @Test
    public void testFractionalRounding() throws DirectiveParseException {
        // 1.9 KB = 1.9 * 1024 = 1945.6 -> should round down to 1945
        Assert.assertEquals(1945L, new ByteSize("1.9KB").getBytes());
        // 0.1 KB = 0.1 * 1024 = 102.4 -> should round down to 102
        Assert.assertEquals(102L, new ByteSize("0.1KB").getBytes());
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidFormatString() throws DirectiveParseException {
        new ByteSize("abc");
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidFormatUnitOnly() throws DirectiveParseException {
        new ByteSize("MB");
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidFormatNegative() throws DirectiveParseException {
        new ByteSize("-10KB");
    }

    @Test(expected = DirectiveParseException.class)
    public void testInvalidFormatUnknownUnit() throws DirectiveParseException {
        new ByteSize("10XB"); // X is not a valid prefix
    }

     @Test(expected = DirectiveParseException.class)
     public void testInvalidFormatMultipleUnits() throws DirectiveParseException {
         new ByteSize("10KBM");
     }

    @Test(expected = DirectiveParseException.class)
    public void testOverflow() throws DirectiveParseException {
        // A value slightly larger than Long.MAX_VALUE / (1024^5) for PB
        // Need to use double for intermediate calc to avoid long overflow before PB multiplication
        double k = 1024.0;
        double m = k * k;
        double g = m * k;
        double t = g * k;
        double p = t * k;
        double nearOverflowPB = ((double) Long.MAX_VALUE / p) + 1;
        new ByteSize(String.format("%.0fPB", nearOverflowPB));
    }
}
