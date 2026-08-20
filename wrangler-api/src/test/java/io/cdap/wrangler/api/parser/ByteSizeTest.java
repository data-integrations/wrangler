/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.wrangler.api.parser;


import org.junit.Test;
import org.junit.Assert;

public class ByteSizeTest {

    @Test
    public void testByteSizeParsing() {
        ByteSize size1 = new ByteSize("10KB");
        Assert.assertEquals(10240L, size1.getBytes());

        ByteSize size2 = new ByteSize("1.5MB");
        Assert.assertEquals(1572864L, size2.getBytes());

        ByteSize size3 = new ByteSize("1GB");
        Assert.assertEquals(1073741824L, size3.getBytes());
    }
}
