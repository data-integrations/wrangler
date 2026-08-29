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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the TimeDuration class.
 */
public class TimeDurationTest {

    @Test
    public void testParseTimeDuration() {
        // Test basic time durations
        TimeDuration timeDuration1 = new TimeDuration("60");
        Assert.assertEquals(60L, timeDuration1.getMilliseconds());
        Assert.assertEquals("60", timeDuration1.getOriginalValue());
        Assert.assertEquals(TokenType.TIME_DURATION, timeDuration1.type());
        Assert.assertEquals(60L, timeDuration1.value().longValue());

        // Test with seconds
        TimeDuration timeDuration2 = new TimeDuration("30s");
        Assert.assertEquals(30L * 1000L, timeDuration2.getMilliseconds());
        Assert.assertEquals("30s", timeDuration2.getOriginalValue());

        // Test with minutes
        TimeDuration timeDuration3 = new TimeDuration("5m");
        Assert.assertEquals(5L * 60L * 1000L, timeDuration3.getMilliseconds());
        Assert.assertEquals("5m", timeDuration3.getOriginalValue());

        // Test with hours
        TimeDuration timeDuration4 = new TimeDuration("2h");
        Assert.assertEquals(2L * 60L * 60L * 1000L, timeDuration4.getMilliseconds());
        Assert.assertEquals("2h", timeDuration4.getOriginalValue());

        // Test with milliseconds
        TimeDuration timeDuration5 = new TimeDuration("500ms");
        Assert.assertEquals(500L, timeDuration5.getMilliseconds());
        Assert.assertEquals("500ms", timeDuration5.getOriginalValue());

        // Test with uppercase units
        TimeDuration timeDuration6 = new TimeDuration("30S");
        Assert.assertEquals(30L * 1000L, timeDuration6.getMilliseconds());
        Assert.assertEquals("30S", timeDuration6.getOriginalValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDuration() {
        new TimeDuration("invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyTimeDuration() {
        new TimeDuration("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullTimeDuration() {
        new TimeDuration(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnsupportedUnit() {
        new TimeDuration("1w");
    }

    @Test
    public void testToJson() {
        TimeDuration timeDuration = new TimeDuration("1h");
        JsonElement json = timeDuration.toJson();

        Assert.assertTrue(json.isJsonObject());
        JsonObject jsonObject = json.getAsJsonObject();

        Assert.assertEquals(TokenType.TIME_DURATION.name(), jsonObject.get("type").getAsString());
        Assert.assertEquals("1h", jsonObject.get("value").getAsString());
        Assert.assertEquals(60L * 60L * 1000L, jsonObject.get("milliseconds").getAsLong());
    }
}