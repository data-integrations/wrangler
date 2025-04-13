/*
 *  Copyright © 2019 Cask Data, Inc.
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

package io.cdap.directives;

import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Time duration in the package.
 */
public class TimeDurationTest {

    @Test
    public void testParsing() {
        Assert.assertEquals("5", new TimeDuration("5ms").value());
        Assert.assertEquals("2100", new TimeDuration("2.1s").value());
        Assert.assertEquals("120000", new TimeDuration("2min").value());
        Assert.assertEquals("7200000", new TimeDuration("2h").value());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeUnit() {
        new TimeDuration("3weeks");
    }
}
