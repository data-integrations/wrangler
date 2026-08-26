/*
 * Copyright 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import io.cdap.api.ByteSize;

public class ByteSizeTest {

    @Test
    public void testParseByteSize() {
        // Valid inputs
        assertEquals(10, ByteSize.parse("10B"));  // Byte
        assertEquals(10240, ByteSize.parse("10KB"));  // Kilobytes
        assertEquals(10485760, ByteSize.parse("10MB"));  // Megabytes
        assertEquals(1073741824, ByteSize.parse("1GB"));  // Gigabytes
        assertEquals(10737418240L, ByteSize.parse("10GB"));  // 10GB

        // Test float-based input
        assertEquals(1048576, ByteSize.parse("1MB"));
        assertEquals(1536, ByteSize.parse("1.5KB"));  // Floating point value in KB
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize() {
        // Invalid inputs
        ByteSize.parse("invalidSize");  // Should throw IllegalArgumentException
    }
}
