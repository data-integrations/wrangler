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

public class TimeDurationTest {

    @Test
    public void testValidDurations() {
        Assert.assertEquals(5000000L, new TimeDuration("5ms").getNanos());
        Assert.assertEquals(1500000L, new TimeDuration("1.5ms").getNanos());
        Assert.assertEquals(2100000000L, new TimeDuration("2.1s").getNanos());
        Assert.assertEquals(1000L, new TimeDuration("1000ns").getNanos());
    }

    @Test
    public void testCaseInsensitiveUnits() {
        Assert.assertEquals(1000000L, new TimeDuration("1Ms").getNanos());
        Assert.assertEquals(2000000000L, new TimeDuration("2S").getNanos());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new TimeDuration("1xyz");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyInput() {
        new TimeDuration("");
    }
}
