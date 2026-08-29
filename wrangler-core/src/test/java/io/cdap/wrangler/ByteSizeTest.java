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

package io.cdap.wrangler;

import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testBytes() {
        ByteSize b = new ByteSize("100B");
        Assert.assertEquals(100, b.getBytes());
    }

    @Test
    public void testKB() {
        ByteSize b = new ByteSize("1KB");
        Assert.assertEquals(1024, b.getBytes());
    }

    @Test
    public void testMB() {
        ByteSize b = new ByteSize("1.5MB");
        Assert.assertEquals(1572864, b.getBytes());
    }

    @Test
    public void testGB() {
        ByteSize b = new ByteSize("2GB");
        Assert.assertEquals(2L * 1024 * 1024 * 1024, b.getBytes());
    }

    @Test
    public void testTB() {
        ByteSize b = new ByteSize("1TB");
        Assert.assertEquals(1L * 1024 * 1024 * 1024 * 1024, b.getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFormat() {
        new ByteSize("15XY");
    }
}
