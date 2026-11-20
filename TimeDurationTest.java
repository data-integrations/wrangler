/*
 * Copyright © 2025 Cask Data, Inc.
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

import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class TimeDurationTest {
    public static void main(String[] args) {
        // Test Case 1: Valid input with milliseconds
        try {
            TimeDuration duration = new TimeDuration("250ms");
            System.out.println("Parsed milliseconds (250ms): " + duration.getMilliseconds());
        } catch (IllegalArgumentException e) {
            System.err.println("Test Case 1 Failed: " + e.getMessage());
        }

        // Test Case 2: Valid input with seconds
        try {
            TimeDuration duration = new TimeDuration("2.5s");
            System.out.println("Parsed milliseconds (2.5s): " + duration.getMilliseconds());
        } catch (IllegalArgumentException e) {
            System.err.println("Test Case 2 Failed: " + e.getMessage());
        }

        // Test Case 3: Valid input with minutes
        try {
            TimeDuration duration = new TimeDuration("1.5m");
            System.out.println("Parsed milliseconds (1.5m): " + duration.getMilliseconds());
        } catch (IllegalArgumentException e) {
            System.err.println("Test Case 3 Failed: " + e.getMessage());
        }

        // Test Case 4: Valid input with hours
        try {
            TimeDuration duration = new TimeDuration("1h");
            System.out.println("Parsed milliseconds (1h): " + duration.getMilliseconds());
        } catch (IllegalArgumentException e) {
            System.err.println("Test Case 4 Failed: " + e.getMessage());
        }

        // Test Case 5: Invalid unit
        try {
            TimeDuration duration = new TimeDuration("2.3xy");
            System.out.println("Parsed milliseconds (2.3xy): " + duration.getMilliseconds());
        } catch (IllegalArgumentException e) {
            System.err.println("Test Case 5 Passed: " + e.getMessage());
        }

        // Test Case 6: Invalid number format
        try {
            TimeDuration duration = new TimeDuration("abcms");
            System.out.println("Parsed milliseconds (abcms): " + duration.getMilliseconds());
        } catch (IllegalArgumentException e) {
            System.err.println("Test Case 6 Passed: " + e.getMessage());
        }

        // Test Case 7: Empty input
        try {
            TimeDuration duration = new TimeDuration("");
            System.out.println("Parsed milliseconds (empty): " + duration.getMilliseconds());
        } catch (IllegalArgumentException e) {
            System.err.println("Test Case 7 Passed: " + e.getMessage());
        }

        // Test Case 8: Null input
        try {
            TimeDuration duration = new TimeDuration(null);
            System.out.println("Parsed milliseconds (null): " + duration.getMilliseconds());
        } catch (IllegalArgumentException e) {
            System.err.println("Test Case 8 Passed: " + e.getMessage());
        }
    }

    @Test
    public void testValidMilliseconds() {
        TimeDuration duration = new TimeDuration("250ms");
        assertEquals(250, duration.getMilliseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new TimeDuration("2.3xy");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput() {
        new TimeDuration(null);
    }
}
