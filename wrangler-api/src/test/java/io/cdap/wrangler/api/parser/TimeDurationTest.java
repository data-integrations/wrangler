/*
 * Copyright © 2017-2025 Cask Data, Inc.
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

/**
 * Unit tests for {@link TimeDuration} parsing.
 */
public class TimeDurationTest {

    @Test
    public void testTimeDurationParsing() throws Exception {
        TimeDuration duration1 = new TimeDuration("5ms");
        Assert.assertEquals("5ms", duration1.value());
        Assert.assertEquals(5 * 1_000_000L, duration1.getNanos());

        TimeDuration duration2 = new TimeDuration("2.1s");
        Assert.assertEquals("2.1s", duration2.value());
        Assert.assertEquals((long) (2.1 * 1_000_000_000L), duration2.getNanos());

        TimeDuration duration3 = new TimeDuration("0ns");
        Assert.assertEquals("0ns", duration3.value());
        Assert.assertEquals(0L, duration3.getNanos());

        TimeDuration duration4 = new TimeDuration("10 d");
        Assert.assertEquals("10 d", duration4.value());
        Assert.assertEquals(10L * 24 * 60 * 60 * 1_000_000_000L, duration4.getNanos());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDurationFormat() throws Exception {
        new TimeDuration("10xs");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyNumber() throws Exception {
        new TimeDuration(".ms");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput() throws Exception {
        new TimeDuration(null);
    }
}