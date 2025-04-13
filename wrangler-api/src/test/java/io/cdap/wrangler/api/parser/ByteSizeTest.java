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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ByteSize} parsing.
 */
public class ByteSizeTest {

    /**
     * Tests parsing of byte size values like "10kb" and "1.5MB".
     *
     * @throws Exception if parsing fails
     */
    @Test
    public void testByteSizeParsing() throws Exception {
        ByteSize size1 = new ByteSize("10kb");
        Assert.assertEquals("10kb", size1.value());
        Assert.assertEquals(10 * 1024L, size1.getBytes());

        ByteSize size2 = new ByteSize("1.5MB");
        Assert.assertEquals("1.5MB", size2.value());
        Assert.assertEquals((long) (1.5 * 1024 * 1024), size2.getBytes());
    }

    /**
     * Tests that invalid byte size formats throw IllegalArgumentException.
     *
     * @throws Exception if parsing does not throw the expected exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize() throws Exception {
        new ByteSize("10XB");
    }
}