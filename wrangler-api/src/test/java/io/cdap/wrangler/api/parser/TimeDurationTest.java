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


public class TimeDurationTest {

    @Test
    public void testValidDurations() {
        Assert.assertEquals(1000.0, new TimeDuration("1s").getTime(), 0.01);
        Assert.assertEquals(1000.0, new TimeDuration("1sec").getTime(), 0.01);
        Assert.assertEquals(1000.0, new TimeDuration("1secs").getTime(), 0.01);
        Assert.assertEquals(100.0, new TimeDuration("100ms").getTime(), 0.01);
        Assert.assertEquals(60000.0, new TimeDuration("1m").getTime(), 0.01);
        Assert.assertEquals(60000.0, new TimeDuration("1min").getTime(), 0.01);
        Assert.assertEquals(3600000.0, new TimeDuration("1h").getTime(), 0.01);
        Assert.assertEquals(3600000.0, new TimeDuration("1hr").getTime(), 0.01);
        Assert.assertEquals(3600000.0, new TimeDuration("1hour").getTime(), 0.01);
        Assert.assertEquals(7200000.0, new TimeDuration("2hours").getTime(), 0.01);
    }

    @Test
    public void testDecimalDurations() {
        Assert.assertEquals(1500.0, new TimeDuration("1.5s").getTime(), 0.01);
        Assert.assertEquals(90000.0, new TimeDuration("1.5min").getTime(), 0.01);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidFormat() {
        new TimeDuration("invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new TimeDuration("1d");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeValue() {
        new TimeDuration("-1s");
    }
}
