/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

    @Test
    public void testByteSizeConversion() {
        // "10KB" should be 10 * 1024 bytes.
        ByteSize size1 = new ByteSize("10KB");
        Assert.assertEquals(10 * 1024, size1.getBytes());

        // "1.5MB" should be 1.5 * 1024 * 1024 bytes.
        ByteSize size2 = new ByteSize("1.5MB");
        Assert.assertEquals((long) (1.5 * 1024 * 1024), size2.getBytes());
    }
}