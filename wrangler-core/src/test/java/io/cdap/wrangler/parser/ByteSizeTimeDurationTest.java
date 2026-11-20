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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTimeDurationTest {

    @Test
    public void testByteSizeParsing() {
        Assert.assertEquals(1024L, new ByteSize("1KB").getBytes());
        Assert.assertEquals(1_048_576L, new ByteSize("1MB").getBytes());
        Assert.assertEquals(1_073_741_824L, new ByteSize("1GB").getBytes());
        Assert.assertEquals(1_099_511_627_776L, new ByteSize("1TB").getBytes());
    }

    @Test
    public void testTimeDurationParsing() {
        Assert.assertEquals(5_000_000L, new TimeDuration("5ms").getMillis());
        Assert.assertEquals(60_000_000_000L, new TimeDuration("1m").getMillis());
        Assert.assertEquals(3_600_000_000_000L, new TimeDuration("1h").getMillis());
    }
}