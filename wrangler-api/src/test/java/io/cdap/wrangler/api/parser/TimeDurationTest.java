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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimeDurationTest {

    @Test
    public void testTimeDurationParsing() {
        TimeDuration duration1 = new TimeDuration("10ms");
        assertEquals(10.0, duration1.getMilliseconds(), 0.0001);
        assertEquals(0.01, duration1.getSeconds(), 0.0001);

        TimeDuration duration2 = new TimeDuration("1.5s");
        assertEquals(1500.0, duration2.getMilliseconds(), 0.0001);
        assertEquals(1.5, duration2.getSeconds(), 0.0001);

        TimeDuration duration3 = new TimeDuration("2m");
        assertEquals(2 * 60 * 1000.0, duration3.getMilliseconds(), 0.0001);
        assertEquals(2 * 60.0, duration3.getSeconds(), 0.0001);
        assertEquals(2.0, duration3.getMinutes(), 0.0001);

        TimeDuration duration4 = new TimeDuration("1.5h");
        assertEquals(1.5 * 60 * 60 * 1000.0, duration4.getMilliseconds(), 0.0001);
        assertEquals(1.5 * 60 * 60.0, duration4.getSeconds(), 0.0001);
        assertEquals(1.5 * 60.0, duration4.getMinutes(), 0.0001);
        assertEquals(1.5, duration4.getHours(), 0.0001);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDuration1() {
        new TimeDuration("10");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDuration2() {
        new TimeDuration("ms");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDuration3() {
        new TimeDuration("10msms");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDuration4() {
        new TimeDuration("abcms");
    }

    @Test
    public void testTokenType() {
        TimeDuration duration = new TimeDuration("10ms");
        assertEquals(TokenType.TIME_DURATION, duration.type());
    }

    @Test
    public void testValue() {
        TimeDuration duration = new TimeDuration("10ms");
        assertEquals("10ms", duration.value());
    }
}
