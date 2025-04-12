/*
 * Copyright 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import io.cdap.api.TimeDuration;

public class TimeDurationTest {

    @Test
    public void testParseTimeDuration() {
        // Valid inputs
        assertEquals(1000, TimeDuration.parse("1s"));  // 1 second in milliseconds
        assertEquals(5000, TimeDuration.parse("5s"));  // 5 seconds
        assertEquals(100, TimeDuration.parse("100ms"));  // 100 milliseconds
        assertEquals(2100, TimeDuration.parse("2.1s"));  // 2.1 seconds
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeDuration() {
        // Invalid input
        TimeDuration.parse("invalidDuration");  // Should throw IllegalArgumentException
    }
}
