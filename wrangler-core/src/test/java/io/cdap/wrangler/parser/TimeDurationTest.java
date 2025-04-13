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

import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class TimeDurationTest {

    @Test
    public void testTimeDurationConversion() {
        // "150ms" should give 150 milliseconds.
        TimeDuration duration1 = new TimeDuration("150ms");
        Assert.assertEquals(150, duration1.getMilliseconds());

        // "2.1s" should convert to 2100 milliseconds.
        TimeDuration duration2 = new TimeDuration("2.1s");
        Assert.assertEquals((long) (2.1 * 1000), duration2.getMilliseconds());
    }
}