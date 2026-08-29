/*
 * Copyright © 2025 Cask Data, Inc.
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

import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Test;
import static org.junit.Assert.*;

public class ByteSizeTest {

    @Test
    public void testValidByteSizes() {
        assertEquals(1536000L, new ByteSize("1.5MB").getBytes());
        assertEquals(1024L, new ByteSize("1KB").getBytes());
        assertEquals(1L, new ByteSize("1B").getBytes());
        assertEquals(1073741824L, new ByteSize("1GB").getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new ByteSize("1.5XY");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNumber() {
        new ByteSize("ABCMB");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyInput() {
        new ByteSize("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput() {
        new ByteSize(null);
    }

    @Test
    public void testValidDecimalByteSizes() {
        assertEquals(1572864L, new ByteSize("1.5MB").getBytes());
        assertEquals(1536L, new ByteSize("1.5KB").getBytes());
    }

    @Test
    public void testValidLowerCaseUnits() {
        assertEquals(1536000L, new ByteSize("1.5mb").getBytes());
        assertEquals(1024L, new ByteSize("1kb").getBytes());
        assertEquals(1L, new ByteSize("1b").getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhitespaceInput() {
        new ByteSize("  ");
    }

    @Test
    public void testValidInputWithSpaces() {
        assertEquals(1536000L, new ByteSize(" 1.5 MB ").getBytes());
    }
}
