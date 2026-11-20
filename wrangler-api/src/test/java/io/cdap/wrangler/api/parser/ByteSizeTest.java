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

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testBasicUnits() {
        Assert.assertEquals(10, new ByteSize("10B").getBytes());
        Assert.assertEquals(1024, new ByteSize("1KB").getBytes());
        Assert.assertEquals(1572864, new ByteSize("1.5MB").getBytes());
        Assert.assertEquals(1073741824L, new ByteSize("1GB").getBytes());
        Assert.assertEquals(1099511627776L, new ByteSize("1TB").getBytes());
    }

    @Test
    public void testCaseInsensitive() {
        Assert.assertEquals(2048, new ByteSize("2kb").getBytes());
        Assert.assertEquals(2097152, new ByteSize("2Mb").getBytes());
        Assert.assertEquals(3221225472L, new ByteSize("3gB").getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new ByteSize("123ABC");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyInput() {
        new ByteSize("");
    }
}
