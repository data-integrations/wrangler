/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations
 * under the License.
 */

package io.cdap.wrangler.api.parser;

import org.junit.Test;
import static org.junit.Assert.*;

public class ByteSizeAndTimeDurationTest {

    @Test
    public void testByteSizeParsing() {
        ByteSize byteSize1 = new ByteSize("10KB");
        assertEquals(10 * 1024, byteSize1.getBytes());

        ByteSize byteSize2 = new ByteSize("2MB");
        assertEquals(2 * 1024 * 1024, byteSize2.getBytes());

        ByteSize byteSize3 = new ByteSize("5GB");
        assertEquals(5L * 1024 * 1024 * 1024, byteSize3.getBytes());
        
        ByteSize byteSize4 = new ByteSize("1TB");
        assertEquals(1L * 1024 * 1024 * 1024 * 1024, byteSize4.getBytes());

    //  ByteSize byteSize5 = new ByteSize("10PB");
    //  assertEquals(10L * 1024 * 1024 * 1024 * 1024 * 1024, byteSize5.getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testByteSizeParsingInvalid() {
        new ByteSize("5XYZ");  // Invalid unit should throw exception
    }

    @Test(expected = IllegalArgumentException.class)
    public void testByteSizeNegativeValue() {
        new ByteSize("-5KB");  // Negative byte size should throw exception
    }

    @Test
    public void testTimeDurationParsing() {
        assertEquals(100000000, TimeDuration.parse("100ms"));
        assertEquals(5000000000L, TimeDuration.parse("5s"));
        assertEquals(7200000000000L, TimeDuration.parse("2h"));
        assertEquals(86400000000000L, TimeDuration.parse("1d"));
    }


}
