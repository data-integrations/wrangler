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
package io.cdap.wrangler;

import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

    @Test
    public void testTimeParsing() {
        Assert.assertEquals(5, new TimeDuration("5ms").getMillis());
        Assert.assertEquals(2000, new TimeDuration("2s").getMillis());
        Assert.assertEquals(120000, new TimeDuration("2m").getMillis());
        Assert.assertEquals(3600000, new TimeDuration("1h").getMillis());
        Assert.assertEquals(90000, new TimeDuration("1.5m").getMillis());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDuration() {
        new TimeDuration("xyz");
    }
}