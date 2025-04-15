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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class GrammarBasedParserByteSizeAndTimeTest {

    @Test
    public void testByteSize() throws Exception {
        // Test ByteSize directly since we can't access tokens from the parser
        ByteSize byteSize = new ByteSize("10KB", 1, 0);
        Assert.assertEquals(10 * 1024L, byteSize.getBytes());

        ByteSize megabytes = new ByteSize("5MB", 1, 0);
        Assert.assertEquals(5 * 1024 * 1024L, megabytes.getBytes());
    }

    @Test
    public void testTimeDuration() throws Exception {
        // Test TimeDuration directly since we can't access tokens from the parser
        TimeDuration ms = new TimeDuration("150ms", 1, 0);
        Assert.assertEquals(150 * 1000000L, ms.getNanoseconds());

        TimeDuration sec = new TimeDuration("5s", 1, 0);
        Assert.assertEquals(5 * 1000000000L, sec.getNanoseconds());
    }
}