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
package io.cdap.directives.parser;

public class TimeDurationParser {

    public static long parse(String input) {
        input = input.trim().toLowerCase();

        if (input.endsWith("ms")) {
            return Long.parseLong(input.replace("ms", "").trim());
        } else if (input.endsWith("s")) {
            return Long.parseLong(input.replace("s", "").trim()) * 1000;
        } else if (input.endsWith("m")) {
            return Long.parseLong(input.replace("m", "").trim()) * 60 * 1000;
        } else if (input.endsWith("h")) {
            return Long.parseLong(input.replace("h", "").trim()) * 60 * 60 * 1000;
        } else {
            throw new IllegalArgumentException("Invalid time duration format: " + input);
        }
    }

    public static String format(long millis, String unit) {
        switch (unit.toLowerCase()) {
            case "milliseconds":
            case "ms":
                return millis + "ms";
            case "seconds":
            case "s":
                return (millis / 1000.0) + "s";
            case "minutes":
            case "m":
                return (millis / (60 * 1000.0)) + "min";
            case "hours":
            case "h":
                return (millis / (60 * 60 * 1000.0)) + "h";
            default:
                throw new IllegalArgumentException("Unsupported output time unit: " + unit);
        }
    }

}