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

public class ByteSizeTest {

    @Test
    public void testByteSizeParsing() {
        ByteSize bs1 = new ByteSize("1024B");
        Assert.assertEquals(1024L, bs1.getBytes());

        ByteSize bs2 = new ByteSize("1KB");
        Assert.assertEquals(1024L, bs2.getBytes());

        ByteSize bs3 = new ByteSize("1.5MB");
        long expected = (long) (1.5 * 1024 * 1024);
        Assert.assertEquals(expected, bs3.getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidByteSize() {
        new ByteSize("5XB");
    }
}
