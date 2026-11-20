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

package io.cdap.wrangler.api.parser;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteSizeTest {

    @Test
    public void testByteSizeParsing() {
        ByteSize size1 = new ByteSize("10KB");
        assertEquals(10 * 1024, size1.getBytes());
        assertEquals(10, size1.getKilobytes(), 0.0001);
        assertEquals(10.0 / 1024, size1.getMegabytes(), 0.0001);

        ByteSize size2 = new ByteSize("1.5MB");
        assertEquals((long) (1.5 * 1024 * 1024), size2.getBytes());
        assertEquals(1.5 * 1024, size2.getKilobytes(), 0.0001);
        assertEquals(1.5, size2.getMegabytes(), 0.0001);

        ByteSize size3 = new ByteSize("2GB");
        assertEquals(2L * 1024 * 1024 * 1024, size3.getBytes());
        assertEquals(2 * 1024 * 1024, size3.getKilobytes(), 0.0001);
        assertEquals(2 * 1024, size3.getMegabytes(), 0.0001);
        assertEquals(2, size3.getGigabytes(), 0.0001);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize1() {
        new ByteSize("10");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize2() {
        new ByteSize("10K");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize3() {
        new ByteSize("10KBKB");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize4() {
        new ByteSize("abcKB");
    }

    @Test
    public void testTokenType() {
        ByteSize size = new ByteSize("10KB");
        assertEquals(TokenType.BYTE_SIZE, size.type());
    }

    @Test
    public void testValue() {
        ByteSize size = new ByteSize("10KB");
        assertEquals("10KB", size.value());
    }
}

