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
 * Test for {@link TimeDuration} token.
 */
public class TimeDurationTest {
    @Test
    public void testParseNanoseconds() {
        // Test basic units
        Assert.assertEquals(1L, new TimeDuration("1ns").getNanoseconds());
        Assert.assertEquals(1000L, new TimeDuration("1us").getNanoseconds());
        Assert.assertEquals(1000L * 1000L, new TimeDuration("1ms").getNanoseconds());
        Assert.assertEquals(1000L * 1000L * 1000L, new TimeDuration("1s").getNanoseconds());
        Assert.assertEquals(60L * 1000L * 1000L * 1000L, new TimeDuration("1m").getNanoseconds());
        Assert.assertEquals(60L * 60L * 1000L * 1000L * 1000L, new TimeDuration("1h").getNanoseconds());
        Assert.assertEquals(24L * 60L * 60L * 1000L * 1000L * 1000L, new TimeDuration("1d").getNanoseconds());

        // Test decimal values
        Assert.assertEquals(500L, new TimeDuration("0.5us").getNanoseconds());
        Assert.assertEquals(1500L, new TimeDuration("1.5us").getNanoseconds());
        Assert.assertEquals(1000L * 1000L * 1.5, new TimeDuration("1.5ms").getNanoseconds(), 0.001);

        // Test case insensitivity
        Assert.assertEquals(1000L * 1000L, new TimeDuration("1MS").getNanoseconds());
        Assert.assertEquals(1000L * 1000L * 1000L, new TimeDuration("1S").getNanoseconds());
        Assert.assertEquals(60L * 1000L * 1000L * 1000L, new TimeDuration("1M").getNanoseconds());

        // Test microsecond symbol
        Assert.assertEquals(1000L, new TimeDuration("1µs").getNanoseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new TimeDuration("1xs");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFormat() {
        new TimeDuration("ms");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNumber() {
        new TimeDuration("abcms");
    }
}
