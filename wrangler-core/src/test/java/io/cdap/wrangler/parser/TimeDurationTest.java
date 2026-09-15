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

import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the TimeDuration token.
 */
public class TimeDurationTest {

    @Test
    public void testDurationParsing() {
        Assert.assertEquals(5000L, new TimeDuration("5s").getMilliseconds());
        Assert.assertEquals(2L, new TimeDuration("2ms").getMilliseconds());
        Assert.assertEquals(60000L, new TimeDuration("1m").getMilliseconds());
        Assert.assertEquals(3600000L, new TimeDuration("1h").getMilliseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new TimeDuration("10lightyears").getMilliseconds();
    }
}
