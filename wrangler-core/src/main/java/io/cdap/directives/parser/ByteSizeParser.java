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

public class ByteSizeParser {
    public static long parse(String input) {
        input = input.trim().toUpperCase();
        if (input.endsWith("KB")) return Long.parseLong(input.replace("KB", "").trim()) * 1024;
        if (input.endsWith("MB")) return Long.parseLong(input.replace("MB", "").trim()) * 1024 * 1024;
        if (input.endsWith("GB")) return Long.parseLong(input.replace("GB", "").trim()) * 1024 * 1024 * 1024;
        if (input.endsWith("B")) return Long.parseLong(input.replace("B", "").trim());
        throw new IllegalArgumentException("Invalid byte size format: " + input);
    }
    public static String format(long bytes, String unit) {
        switch (unit.toUpperCase()) {
            case "B":
                return bytes + "B";
            case "KB":
                return (bytes / 1024.0) + "KB";
            case "MB":
                return (bytes / (1024.0 * 1024)) + "MB";
            case "GB":
                return (bytes / (1024.0 * 1024 * 1024)) + "GB";
            default:
                throw new IllegalArgumentException("Unsupported output size unit: " + unit);
        }
    }
}